import configparser
import json
import os

from time import sleep
import asyncio
from typing import AsyncIterator

from asyncstdlib import enumerate

from solders.rpc.config import RpcTransactionLogsFilterMentions
from solana.rpc.websocket_api import connect
from solana.rpc.commitment import Finalized
from solana.rpc.api import Client
from solana.exceptions import SolanaRpcException
from websockets.exceptions import ConnectionClosedError, ProtocolError
from solana.rpc.websocket_api import SolanaWsClientProtocol
from solders.signature import Signature
from spl.token.core import _TokenCore
from spl.token.instructions import close_account, CloseAccountParams, get_associated_token_address, \
    create_associated_token_account
from solana.rpc.types import TokenAccountOpts
from solana.rpc.api import RPCException
from solana.transaction import Transaction
from solana.rpc.api import Pubkey
from spl.token.client import Token
from solana.rpc.commitment import Commitment

from utils.handle_pool_keys import get_pool_infos
from utils.instruction_api import make_swap_instruction, get_token_account
import base58
from solana.rpc.api import Keypair


async def main():
    async for websocket in connect(wss_url):
        try:
            subscription_id = await subscribe_to_logs(
                websocket,
                RpcTransactionLogsFilterMentions(raydium_lp_v4),
                Finalized
            )
            async for i, signature in enumerate(process_messages(websocket, log_instruction)):  # type: ignore
                try:
                    transaction = solana_client.get_transaction(
                        signature,
                        encoding="jsonParsed",
                        max_supported_transaction_version=0
                    )
                    instructions = transaction.value.transaction.transaction.message.instructions
                    filtered_instuctions = [instruction for instruction in instructions if
                                            instruction.program_id == raydium_lp_v4]
                    for instruction in filtered_instuctions:
                        accounts = instruction.accounts
                        print(f'token name: {accounts[8].__str__()}')
                        print(f"dex trade, https://dexscreener.com/solana/{str(accounts[8])}")
                        print(f"signature info, https://solscan.io/tx/{signature}")

                        if is_buy:
                            private_key_bytes = base58.b58decode(private_key_string)
                            payer = Keypair.from_bytes(private_key_bytes)
                            pool_keys = get_pool_infos(solana_client, accounts, payer)
                            if pool_keys.get('status_code') != 0:
                                print(pool_keys.get('msg'))
                                break
                            pool_keys = pool_keys.get('msg')
                            result = solana_client.get_account_info_json_parsed(
                                Pubkey.from_string(pool_keys.get('quoteVault'))).value.lamports
                            lamport_per_sol = 1000000000
                            pool_number = result / lamport_per_sol
                            print(f'pool size: {pool_number}')

                            if not os.path.exists('pool_infos.json'):
                                with open('pool_infos.json', 'w') as fw:
                                    fw.write('[]')
                            with open('pool_infos.json', 'r') as fw:
                                contents = json.load(fw)
                            contents.append(pool_keys)
                            with open('pool_infos.json', 'w') as fw:
                                json.dump(contents, fw)

                            if pool_number > float(pool_size):
                                amount = float(sol_amount)
                                buy_task = asyncio.create_task(
                                    buy(solana_client, accounts[8].__str__(), payer, amount, pool_keys))
                                result = await buy_task
                                if result:
                                    print("create buy order")
                                    await asyncio.sleep(int(wait_seconds))
                                    if is_sell:
                                        for _ in range(5):
                                            sell_task = asyncio.create_task(
                                                sell(solana_client, accounts[8].__str__(), payer, pool_keys))
                                            result = await sell_task
                                            if result:
                                                print('sell order finished')
                                                break
                                            else:
                                                print('sold error.')
                                    else:
                                        print('auto sell switch close')
                                else:
                                    print('create buy order failed')
                        else:
                            print("auto buy switch close")

                    break
                except SolanaRpcException as err:
                    print(f"sleep for 5 seconds and try again, error information: {err}")
                    sleep(5)
                    continue
        except (ProtocolError, ConnectionClosedError) as err:
            continue
        except KeyboardInterrupt:
            if websocket:
                await websocket.logs_unsubscribe(subscription_id)


async def subscribe_to_logs(websocket: SolanaWsClientProtocol,
                            mentions: RpcTransactionLogsFilterMentions,
                            commitment: Commitment) -> int:
    await websocket.logs_subscribe(
        filter_=mentions,
        commitment=commitment
    )
    first_resp = await websocket.recv()
    return first_resp[0].result  # type: ignore


async def process_messages(websocket: SolanaWsClientProtocol,
                           instruction: str) -> AsyncIterator[Signature]:
    async for idx, msg in enumerate(websocket):
        value = msg[0].result.value
        if not idx % 100:
            print(f'idx: {idx}')
        for log in value.logs:
            if instruction not in log:
                continue
            yield value.signature


def transfer_pool_keys_to_pk(pool_keys: dict):
    tf_pool_keys = {}
    for name, value in pool_keys.items():
        if isinstance(value, int):
            tf_pool_keys[name] = value
        else:
            tf_pool_keys[name] = Pubkey.from_string(value)

    return tf_pool_keys


async def buy(solana_client, token_mint, payer, amount, pool_keys=None):
    amount_in = int(amount * 1000000000)
    mint = Pubkey.from_string(token_mint)
    pool_keys = transfer_pool_keys_to_pk(pool_keys)
    accountProgramId = solana_client.get_account_info_json_parsed(mint)
    TOKEN_PROGRAM_ID = accountProgramId.value.owner
    swap_associated_token_address, swap_token_account_Instructions = get_token_account(solana_client, payer.pubkey(),
                                                                                       mint)

    balance_needed = Token.get_min_balance_rent_for_exempt_for_account(solana_client)
    new_pair_pk, swap_tx, payer, new_pair, opts, = _TokenCore._create_wrapped_native_account_args(
        TOKEN_PROGRAM_ID, payer.pubkey(), payer, amount_in,
        False, balance_needed, Commitment("confirmed"))

    instructions_swap = make_swap_instruction(amount_in,
                                              new_pair_pk,
                                              swap_associated_token_address,
                                              pool_keys,
                                              mint,
                                              solana_client,
                                              payer
                                              )

    params = CloseAccountParams(account=new_pair_pk, dest=payer.pubkey(), owner=payer.pubkey(),
                                program_id=TOKEN_PROGRAM_ID)
    closeAcc = (close_account(params))
    if swap_token_account_Instructions != None:
        swap_tx.add(swap_token_account_Instructions)
    swap_tx.add(instructions_swap)
    swap_tx.add(closeAcc)

    try:
        txn = solana_client.send_transaction(swap_tx, payer, new_pair)
        return True
    except:
        return


async def sell(client, token_mint, payer=None, pool_keys=None):
    tokenPk = Pubkey.from_string(str(token_mint))
    sol = Pubkey.from_string("So11111111111111111111111111111111111111112")

    pool_keys = transfer_pool_keys_to_pk(pool_keys)
    account_program_id = client.get_account_info_json_parsed(tokenPk)
    program_id_of_token = account_program_id.value.owner
    if not payer:
        return
    accounts = client.get_token_accounts_by_owner_json_parsed(payer.pubkey(), TokenAccountOpts(
        program_id=program_id_of_token)).value
    amount_in = 0
    for account in accounts:
        mint_in_acc = account.account.data.parsed['info']['mint']
        if mint_in_acc == str(tokenPk):
            amount_in = int(account.account.data.parsed['info']['tokenAmount']['amount'])
            break
    # 卖出数量判断
    if amount_in == 0:
        return
    account_data = client.get_token_accounts_by_owner(payer.pubkey(), TokenAccountOpts(tokenPk))
    if account_data.value:
        swap_token_account = account_data.value[0].pubkey
    else:
        return
    if not swap_token_account:
        return

    try:
        account_data = client.get_token_accounts_by_owner(payer.pubkey(), TokenAccountOpts(sol))
        wsol_token_account = account_data.value[0].pubkey
        wsol_token_account_Instructions = None
    except:
        wsol_token_account = get_associated_token_address(payer.pubkey(), sol)
        wsol_token_account_Instructions = create_associated_token_account(payer.pubkey(), payer.pubkey(), sol)

    instructions_swap = make_swap_instruction(amount_in, swap_token_account, wsol_token_account, pool_keys, tokenPk,
                                              client, payer)
    params = CloseAccountParams(account=wsol_token_account, dest=payer.pubkey(), owner=payer.pubkey(),
                                program_id=program_id_of_token)
    closeAcc = close_account(params)
    swap_tx = Transaction()
    signers = [payer]
    if wsol_token_account_Instructions != None:
        swap_tx.add(wsol_token_account_Instructions)
    swap_tx.add(instructions_swap)
    swap_tx.add(closeAcc)
    try:
        client.send_transaction(swap_tx, *signers)
        return True
    except RPCException as e:
        return


if __name__ == "__main__":
    # config proxy
    os.environ["http_proxy"] = "http://127.0.0.1:7890"
    os.environ["https_proxy"] = "http://127.0.0.1:7890"

    config = configparser.ConfigParser()
    config.read('./config.ini')
    private_key_string = config['user']['private_key']
    is_buy = config['config']['is_buy']
    is_sell = config['config']['is_sell']
    pool_size = config['config']['pool_size']
    sol_amount = config['config']['sol_amount']
    wait_seconds = config['config']['wait_seconds']
    main_url = config['solanaConfig']['main_url']
    wss_url = config['solanaConfig']['wss_url']
    raydium_lp_v4 = config['solanaConfig']['raydium_lp_v4']
    log_instruction = config['solanaConfig']['log_instruction']

    solana_client = Client(main_url)
    raydium_lp_v4 = Pubkey.from_string(raydium_lp_v4)
    print(f"start solana sniper...")
    asyncio.run(main())
