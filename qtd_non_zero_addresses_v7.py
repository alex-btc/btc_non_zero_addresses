  # obter uma série temporal da quantidade de endereços com saldo maior do que zero.
  # voce tem que ter tanto o bitcoind quanto o fulcrum electrum server rodando.

  from datetime import datetime
  import json
  import os
  from os.path import expanduser
  from tqdm import tqdm
  import numpy as np
  import pandas as pd
  from logging import error
  import bitcoinrpc.authproxy # tem que instalar com pip install python-bitcoinrpc
  from collections import defaultdict
  import hashlib
  import socket
  from pycoin.symbols.btc import network
  from multiprocessing import Pool

  #############
  # PARAMETROS:
  #############

  # altere aqui o rpcuser e rpcpassword do seu arquivo bitcoin.conf
  username = 'XXXXX'
  password = 'XXXXX'

  host = '192.168.1.110' #trocar aqui pro ip do electrum server e do bitcoind.
  port_electrum_server = 50001
  port_bitcoind = 8332

  # somente analisa transacoes cujo output seja maior que (pra ganhar tempo):
  qtd_btc_minima_para_analisar_tx = 0.011
  threshold_min_btc = 0.011

  # grava dados a cada qtd_blocos_gravar blocos:
  qtd_blocos_gravar = 500

  # diretorio onde os dados serao gravados:
  data_path = expanduser("~") + '/data/'

  #############

  # classe para interagir com o bitcoind (obrigado Erik!):
  class RPC:
    rpc = None
    def __init__(self, address = host, port = port_bitcoind, username = "", password = "", cookie = "", timeout = 1000):

      if (username != "" and username != None) and (password != None and password != ""):
        conString = "http://%s:%s@%s:%s" % (username, password, address, port)
      elif (cookie != "" and cookie != None):
        conString = "http://%s@%s%s" % (cookie, address, port)
      else:
        raise error("Missign authentication method!")
      try:
        self.rpc = bitcoinrpc.authproxy.AuthServiceProxy(conString)
        self.getbestBlockhash()
      except ConnectionRefusedError:
        print ("Connection refused! Is your bitcoind running?")
        exit(1)
      except bitcoinrpc.authproxy.JSONRPCException:
        print("Authentication error. Check your credentials")
        exit(1)

    def getbestBlockhash(self):
      try:
        return self.rpc.getbestblockhash()
      except ConnectionRefusedError:
        print ("Connection refused! Is your bitcoind running?")
        exit(1)
      except bitcoinrpc.authproxy.JSONRPCException:
        print("Authentication error. Check your credentials")
        exit(1)  
    
    def getBlock(self, blockHash):
      if blockHash == None or len(blockHash) != 64:
        raise error("Invalid block hash")
      try:
        return self.rpc.getblock(blockHash)
      except ConnectionRefusedError:
        print ("Connection refused! Is your bitcoind running?")
        exit(1)
      except bitcoinrpc.authproxy.JSONRPCException:
        print("Authentication error. Check your credentials")
        exit(1)  

    def getBlockWithTransactions(self, blockHash):
      if blockHash == None or len(blockHash) != 64:
        raise error("Invalid block hash")
      try:
        return self.rpc.getblock(blockHash, 2)
      except ConnectionRefusedError:
        print ("Connection refused! Is your bitcoind running?")
        exit(1)
      except bitcoinrpc.authproxy.JSONRPCException:
        print("Authentication error. Check your credentials")
        exit(1)

    def getBlockHashByHeight(self, height):
      if height < 0:
        raise error ("Invalid block height")
      try:
        return self.rpc.getblockhash(height)
      except ConnectionRefusedError:
        print ("Connection refused! Is your bitcoind running?")
        exit(1)
      except bitcoinrpc.authproxy.JSONRPCException:
        print("Authentication error. Check your credentials")
        exit(1)  
    
  # funcao que vai ser jogada no pool de CPUs, recebe uma transacao e retorna o saldo dos addresses envolvidos nela:
  def main_loop(tx):

    new_addresses = {}
    tx_total_output_value = 0

    # soma saldos e descobre inputs:
    for output in tx['vout']:

        if 'addresses' in output['scriptPubKey'].keys():
            if len(output['scriptPubKey']['addresses']) > 1:
              print('  DEVE TER MAIS OUTPUT AQUI, VERIFICAR!!!   ')
              print(tx)
            address = output['scriptPubKey']['addresses'][0]
            value_to_add = float(output['value'])
            tx_total_output_value += value_to_add
            if address in new_addresses.keys():
                new_addresses[address] += value_to_add
            else:
                new_addresses[address] = value_to_add
        else:
            if 'address' in output['scriptPubKey'].keys():
              address = output['scriptPubKey']['address']
              value_to_add = float(output['value'])
              tx_total_output_value += value_to_add
              if address in new_addresses.keys():
                  new_addresses[address] += value_to_add
              else:
                  new_addresses[address] = value_to_add

    # diminui saldo:
    inputs = {}

    for input in tx['vin']:
        if 'coinbase' in input.keys(): # block reward, nao tem output previo:
            inputs['coinbase'] = 'coinbase'
        else:
            inputs[input['txid']] = input['vout'] # txid e o index dos outputs dela que gerou esse input

    # soh analisa os inputs (passo bem demorado), se tx tiver valor minimo:
    if tx_total_output_value >= qtd_btc_minima_para_analisar_tx:

      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      sock.connect((host, port_electrum_server))

      for input in inputs.items():

        if input[1] != 'coinbase': # se eh transacao coinbase nao ha o que diminuir.
            content = {
            "method": "blockchain.transaction.get",
            "params": [input[0], True], # [tx_hash, verbose 1 daih retorna json senao retorna raw]
            "id": np.random.randint(100000)}

            sock.sendall(json.dumps(content).encode('utf-8') + b'\n')
            res = ""
            while '}}\r\n' not in res[-4:]: #enquanto nao for o fim da string, continua recebendo:
                data = sock.recv(1024)
                res += data.decode()
            tx_temp = json.loads(res)['result']
            index = input[1]
            value_to_subtract = float(tx_temp['vout'][index]['value']) # usa o index pra encontrar o output certo
            tipo_tx = tx_temp['vout'][index]['scriptPubKey']['type']

            if 'address' in tx_temp['vout'][index]['scriptPubKey']:
              address = tx_temp['vout'][index]['scriptPubKey']['address']
            elif 'addresses' in tx_temp['vout'][index]['scriptPubKey']:
              address = tx_temp['vout'][index]['scriptPubKey']['addresses'][0]
              if len(tx_temp['vout'][index]['scriptPubKey']['addresses']) > 1:
                print('    TEM MAIS DE UM OUTPUT AQUI, VERIFICAR!!!!')
                print(input)

            if address in new_addresses.keys():
                new_addresses[address] -= value_to_subtract
            else:
                new_addresses[address] = -value_to_subtract

      sock.close()
          
    return new_addresses

  # https://stackoverflow.com/questions/16008670/how-to-hash-a-string-into-8-digits
  # https://stackoverflow.com/questions/4567089/hash-function-that-produces-short-hashes

  # descobre o numero do ultimo bloco:
  rpc = RPC(username=username, password=password, port=port_bitcoind, address=host)
  last_blockhash = rpc.getbestBlockhash()
  last_block = rpc.getBlock(last_blockhash)['height']

  # inicializa variaveis:
  qty_non_zero_addresses_per_block = {}
  addresses = defaultdict(float) # saldo de cada endereço, movimenta a cada bloco # valor padrao de uma entrada que nao existe eh zero.
  timestamp = {} # unix timestamp de cada bloco
  qtd_txs_per_block = {} # quantidade de transacoes por bloco, nocao de blocos cheios ou nao

  initial_block = 1
  final_block = last_block # ateh o final

  # para ler dados do disco em caso de interrupcao, para comecar de novo:
  # se jah existir arquivos gravados, le eles: 
  if os.path.isfile(data_path + 'addresses_v5.csv'):
    addresses = pd.read_csv(data_path + 'addresses_v5.csv')
    addresses = pd.Series(addresses.iloc[:,1].values, index = addresses.iloc[:,0].values)
    addresses = addresses.to_dict()
    addresses = defaultdict(float, addresses) # saldo de cada endereço, movimenta a cada bloco # valor padrao de uma entrada que nao existe eh zero.

    initial_block = pd.read_csv(data_path + 'qty_non_zero_addresses_per_block_v5.csv').iloc[-1,0] + 1 # comeca no bloco imediatamente posterior aquele que foi gravado por ultimo.

    qty_non_zero_addresses_per_block = pd.read_csv(data_path + 'qty_non_zero_addresses_per_block_v5.csv')
    qty_non_zero_addresses_per_block = pd.Series(qty_non_zero_addresses_per_block.iloc[:,1].values, index = qty_non_zero_addresses_per_block.iloc[:,0].values)
    qty_non_zero_addresses_per_block = qty_non_zero_addresses_per_block.to_dict()

    timestamp = pd.read_csv(data_path + 'timestamp_v5.csv')
    timestamp = pd.Series(timestamp.iloc[:,1].values, index = timestamp.iloc[:,0].values)
    timestamp = timestamp.to_dict()

    qtd_txs_per_block = pd.read_csv(data_path + 'qtd_txs_per_block_v5.csv')
    qtd_txs_per_block = pd.Series(qtd_txs_per_block.iloc[:,1].values, index = qtd_txs_per_block.iloc[:,0].values)
    qtd_txs_per_block = qtd_txs_per_block.to_dict()

  # inicia a conexao:
  rpc = RPC(username=username, password=password, port=port_bitcoind, address=host)

  blocks = tqdm(range(initial_block, final_block))

  for block in blocks:
    # reinicia a conexao:
    rpc = RPC(username=username, password=password, port=port_bitcoind, address=host)

    # descobre o hash do bloco:
    blockhash = rpc.getBlockHashByHeight(block)

    # pega as transacoes do bloco:
    blockinfo = rpc.getBlockWithTransactions(blockhash)
    txs = blockinfo['tx']

    timestamp[block] = blockinfo['time']
    qtd_txs_per_block[block] = len(txs)

    # descricao do tqdm:
    blocks.set_description("block %s, %s txs, %s-%s-%s" % (block, qtd_txs_per_block[block], datetime.fromtimestamp(timestamp[block]).day, datetime.fromtimestamp(timestamp[block]).month, datetime.fromtimestamp(timestamp[block]).year  ))

    qtd_CPUs = 16 # Fulcrum por padrao aceita no maximo 12 conexoes, tem que alterar em: max_clients_per_ip no arquivo de configuracao
    pool = Pool(qtd_CPUs) # number of CPUs # tem que ser colocada sempre que for usada.

    # passa todas as tx do bloco para a funcao main_loop (paralelizado)
    addresses_from_pool = pool.map(main_loop, txs)

    # gera um dict unico, nao varios dicts em uma lista:
    count = 0
    for add in addresses_from_pool:
      if count == 0:
        addresses_new = pd.Series(add, dtype=float)
      else:
        addresses_new = addresses_new.add(pd.Series(add, dtype=float), fill_value=0)
      count += 1

    # atualiza saldo dos addresses com o resultado do novo bloco:
    for addr in addresses_new.index:
      addresses[addr] = addresses[addr] + addresses_new.loc[addr]
    
    qty_non_zero_addresses_per_block[block] = sum([1 for value in addresses.values() if value > threshold_min_btc]) # mais rapido

    # grava dados a cada qtd_blocos_gravar:
    if block % qtd_blocos_gravar == 0:
        
        # limpa addresses vazios:
        addresses = {address:value for (address, value) in addresses.items() if value > threshold_min_btc} # dah erro de falta de memoria quando dict fica grande.
        addresses = defaultdict(float, addresses) # transforma de novo em defaultdict

        if not os.path.isdir(data_path): # se diretorio ainda nao existe:
          os.mkdir(data_path)

        pd.Series(addresses, dtype=float).to_csv(data_path + 'addresses_v5.csv') 
        pd.Series(qty_non_zero_addresses_per_block,dtype=int).to_csv(data_path + 'qty_non_zero_addresses_per_block_v5.csv')
        pd.Series(timestamp, dtype=int).to_csv(data_path + 'timestamp_v5.csv')
        pd.Series(qtd_txs_per_block, dtype=int).to_csv(data_path + 'qtd_txs_per_block_v5.csv')

        # refaz a conexao, porque a gravacao dos dados acima pode demorar um pouco e aih a conexao cai.
        rpc = RPC(username=username, password=password, port=port_bitcoind, address=host)
