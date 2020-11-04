/*
Name: Ethereum Blockchain syncer
Version: .0.0.2
This file will start syncing the blockchain from the node address you provide in the conf.json file.
Please read the README in the root directory that explains the parameters of this code
*/
require('../db.js');
const BigNumber = require('bignumber.js');
const _ = require('lodash');

const asyncL = require('async');
const Web3 = require('web3');

const ERC20ABI = require('human-standard-token-abi');

const fetch = require('node-fetch');

const mongoose = require('mongoose');
const etherUnits = require('../lib/etherUnits.js');
const { Market } = require('../db.js');

const Block = mongoose.model('Block');
const Transaction = mongoose.model('Transaction');
const Account = mongoose.model('Account');
const Contract = mongoose.model('Contract');
const TokenTransfer = mongoose.model('TokenTransfer');

const ERC20_METHOD_DIC = { '0xa9059cbb': 'transfer', '0xa978501e': 'transferFrom' };

let block_bulkOps = [];
let transaction_bulkOps = [];
let transaction_blocks = 0;
let transaction_miners = [];


/**
  Start config for node connection and sync
**/
/**
 * nodeAddr: node address
 * wsPort:  rpc port
 * bulkSize: size of array in block to use bulk operation
 */
// load config.json
const config = { nodeAddr: 'localhost', wsPort: 8546, bulkSize: 100 };
try {
  var local = require('../config.json');
  _.extend(config, local);
  console.log('config.json found.');
} catch (error) {
  if (error.code === 'MODULE_NOT_FOUND') {
    local = require('../config.example.json');
    _.extend(config, local);
    console.log('No config file found. Using default configuration... (config.example.json)');
  } else {
    throw error;
  }
}

console.log(`Connecting ${config.nodeAddr}:${config.wsPort}...`);
// Sets address for RPC WEB3 to connect to, usually your node IP address defaults ot localhost
const web3 = new Web3(new Web3.providers.WebsocketProvider(`ws://${config.nodeAddr}:${config.wsPort.toString()}`));

const normalizeTX = (txData, receipt, blockData) => {
  const tx = {
    blockHash: txData.blockHash,
    blockNumber: txData.blockNumber,
    from: txData.from.toLowerCase(),
    hash: txData.hash.toLowerCase(),
    value: etherUnits.toEther(new BigNumber(txData.value), 'wei'),
    nonce: txData.nonce,
    r: txData.r,
    s: txData.s,
    v: txData.v,
    gas: txData.gas,
    gasUsed: receipt.gasUsed,
    gasPrice: String(txData.gasPrice),
    input: txData.input,
    transactionIndex: txData.transactionIndex,
    timestamp: blockData.timestamp,
  };

  if (receipt.status) {
    tx.status = receipt.status;
  }

  if (txData.to) {
    tx.to = txData.to.toLowerCase();
    return tx;
  } else if (txData.creates) {
    tx.creates = txData.creates.toLowerCase();
    return tx;
  } else {
    tx.creates = receipt.contractAddress.toLowerCase();
    return tx;
  }
};

/**
  Write the whole block object to DB
**/
const writeBlockToDB = async (config_, blockData, flush) => {
  if (!block_bulkOps) {
    block_bulkOps = [];
  }
  if (blockData && blockData.number >= 0) {
    block_bulkOps.push(new Block(blockData));
    if (!('quiet' in config_ && config_.quiet === true)) {
      console.log(`\t- block #${blockData.number.toString()} inserted.`);
    }
  }

  if (flush && block_bulkOps.length > 0 || block_bulkOps.length >= config_.bulkSize) {
    const bulk = block_bulkOps;
    block_bulkOps = [];
    if (bulk.length === 0) return;

    try {
      var blocks = await Block.collection.insertMany(bulk);
      if (!('quiet' in config_ && config_.quiet === true)) {
        console.log(`* ${blocks.insertedCount} blocks successfully written.`);
      }
    } catch (err) {
      if (err.code === 11000) {
        if (!('quiet' in config_ && config_.quiet === true)) {
          console.log(`Skip: Duplicate DB key : ${err}`);
        }
      } else {
        console.log(`Error: Aborted due to error on DB: ${err}`);
        process.exit(9);
      }
    }
  }
};
/**
  Break transactions out of blocks and write to DB
**/
const writeTransactionsToDB = async (config_, blockData, flush) => {
  if (!transaction_bulkOps) {
    transaction_bulkOps = [];
    transaction_blocks = 0;
  }

  // save miner addresses
  if (!transaction_miners) {
    transaction_miners = [];
  }

  if (blockData) {
    transaction_miners.push({ address: blockData.miner, blockNumber: blockData.number, type: 0 });
  }

  if (blockData && blockData.transactions.length > 0) {
    for (var d in blockData.transactions) {
      const txData = blockData.transactions[d];
      const receipt = await web3.eth.getTransactionReceipt(txData.hash);
      const tx = normalizeTX(txData, receipt, blockData);
      // Contact creation tx, Event logs of internal transaction
      if (txData.input && txData.input.length > 2) {
        // Contact creation tx
        if (txData.to === null) {
          // Support Parity & Geth case
          var contractAddress;
          if (txData.creates) {
            contractAddress = txData.creates.toLowerCase();
          } else {
            contractAddress = receipt.contractAddress.toLowerCase();
          }
          const contractdb = {};
          let isTokenContract = true;
          const Token = new web3.eth.Contract(ERC20ABI, contractAddress);
          contractdb.owner = txData.from;
          contractdb.blockNumber = blockData.number;
          contractdb.creationTransaction = txData.hash;
          try {
            const call = await web3.eth.call({ to: contractAddress, data: web3.utils.sha3('totalSupply()') });
            if (call === '0x') {
              isTokenContract = false;
            } else {
              try {
                // ERC20 & ERC223 Token Standard compatible format
                contractdb.tokenName = await Token.methods.name().call();
                contractdb.decimals = await Token.methods.decimals().call();
                contractdb.symbol = await Token.methods.symbol().call();
                contractdb.totalSupply = await Token.methods.totalSupply().call();
              } catch (err) {
                isTokenContract = false;
              }
            }
          } catch (err) {
            isTokenContract = false;
          }
          contractdb.byteCode = await web3.eth.getCode(contractAddress);
          if (isTokenContract) {
            contractdb.ERC = 2;
          } else {
            // Normal Contract
            contractdb.ERC = 0;
          }
          
          // Write to db
          Contract.update(
            { address: contractAddress },
            { $setOnInsert: contractdb },
            { upsert: true },
            (err, data) => {
              if (err) {
                console.log(err);
              }
            },
          );
        } else {
          // Internal transaction  . write to doc of InternalTx
          const transfer = {
            'hash': '', 'blockNumber': 0, 'from': '', 'to': '', 'contract': '', 'value': 0, 'timestamp': 0,
          };
          const methodCode = txData.input.substr(0, 10);
          if (ERC20_METHOD_DIC[methodCode] === 'transfer' || ERC20_METHOD_DIC[methodCode] === 'transferFrom') {
            if (ERC20_METHOD_DIC[methodCode] === 'transfer') {
              // Token transfer transaction
              transfer.from = txData.from;
              transfer.to = `0x${txData.input.substring(34, 74)}`;
              transfer.value = Number(`0x${txData.input.substring(74)}`);
            } else {
              // transferFrom
              transfer.from = `0x${txData.input.substring(34, 74)}`;
              transfer.to = `0x${txData.input.substring(74, 114)}`;
              transfer.value = Number(`0x${txData.input.substring(114)}`);
            }
            transfer.method = ERC20_METHOD_DIC[methodCode];
            transfer.hash = txData.hash;
            transfer.blockNumber = blockData.number;
            transfer.contract = txData.to;
            transfer.timestamp = blockData.timestamp;
            // Write transfer transaction into db
            TokenTransfer.update(
              { hash: transfer.hash },
              { $setOnInsert: transfer },
              { upsert: true },
              (err, data) => {
                if (err) {
                  console.log(err);
                }
              },
            );
          }
        }
      }
      transaction_bulkOps.push(tx);
    }
    if (!('quiet' in config_ && config_.quiet === true)) {
      console.log(`\t- block #${blockData.number.toString()}: ${blockData.transactions.length.toString()} transactions recorded.`);
    }
  }
  transaction_blocks++;

  if (flush && transaction_blocks > 0 || transaction_blocks >= config_.bulkSize) {
    const bulk = transaction_bulkOps;
    transaction_bulkOps = [];
    transaction_blocks = 0;
    const miners = transaction_miners;
    transaction_miners = [];

    // setup accounts
    const data = {};
    bulk.forEach((tx) => {
      data[tx.from] = { address: tx.from, blockNumber: tx.blockNumber, type: 0 };
      if (tx.to) {
        data[tx.to] = { address: tx.to, blockNumber: tx.blockNumber, type: 0 };
      }
    });

    // setup miners
    miners.forEach((miner) => {
      data[miner.address] = miner;
    });

    const accounts = Object.keys(data);

    if (bulk.length === 0 && accounts.length === 0) return;

    // update balances
    if (config_.settings.useRichList && accounts.length > 0) {
      asyncL.eachSeries(accounts, (account, eachCallback) => {
        const { blockNumber } = data[account];
        // get contract account type
        web3.eth.getCode(account, (err, code) => {
          if (err) {
            console.log(`ERROR: fail to getCode(${account})`);
            return eachCallback(err);
          }
          if (code.length > 2) {
            data[account].type = 1; // contract type
          }

          web3.eth.getBalance(account, blockNumber, (err, balance) => {
            if (err) {
              console.log(err);
              console.log(`ERROR: fail to getBalance(${account})`);
              return eachCallback(err);
            }

            data[account].balance = parseFloat(web3.utils.fromWei(balance, 'ether'));
            eachCallback();
          });
        });
      }, (err) => {
        let n = 0;
        accounts.forEach((account) => {
          n++;
          if (!('quiet' in config_ && config_.quiet === true)) {
            if (n <= 5) {
              console.log(` - upsert ${account} / balance = ${data[account].balance}`);
            } else if (n === 6) {
              console.log(`   (...) total ${accounts.length} accounts updated.`);
            }
          }
          // upsert account
          Account.collection.updateOne({ address: account }, { $set: data[account] }, { upsert: true });
        });
      });
    }

    if (bulk.length > 0) {
      Transaction.collection.insertMany(bulk, (err, tx) => {
        if (typeof err !== 'undefined' && err) {
          if (err.code === 11000) {
            if (!('quiet' in config_ && config_.quiet === true)) {
              console.log(`Skip: Duplicate transaction key ${err}`);
            }
          } else {
            console.log(`Error: Aborted due to error on Transaction: ${err}`);
            process.exit(9);
          }
        } else {
          if (!('quiet' in config_ && config_.quiet === true)) {
            console.log(`* ${tx.insertedCount} transactions successfully recorded.`);
          }
        }
      });
    }
  }
};

/**
  //Just listen for latest blocks and sync from the start of the app.
**/
const listenBlocks = function (config_) {
  const newBlocks = web3.eth.subscribe('newBlockHeaders', (error, result) => {
    if (!error) {
      return;
    }

    console.error(error);
  });
  newBlocks.on('data', (blockHeader) => {
    web3.eth.getBlock(blockHeader.hash, true, (error, blockData) => {
      if (blockHeader === null) {
        console.log('Warning: null block hash');
      } else {
        writeBlockToDB(config_, blockData, true);
        writeTransactionsToDB(config_, blockData, true);
      }
    });
  });
  newBlocks.on('error', console.error);
};

/**
  If full sync is checked this function will start syncing the block chain from lastSynced param see README
**/
const syncChain = function (config_, nextBlock) {
  if (web3.eth.net.isListening()) {
    if (typeof nextBlock === 'undefined') {
      prepareSync(config_, (error, startBlock) => {
        if (error) {
          console.log(`ERROR: error: ${error}`);
          return;
        }
        syncChain(config_, startBlock);
      });
      return;
    }

    if (nextBlock === null) {
      console.log('nextBlock is null');
      return;
    }
    if (nextBlock < config_.startBlock) {
      writeBlockToDB(config_, null, true);
      writeTransactionsToDB(config_, null, true);
      console.log('*** Sync Finsihed ***');
      config_.syncAll = false;
      return;
    }

    let done = 0;
    function processBlock(error, blockData) {
      if (error) {
        console.log(`Warning (syncChain): error on getting block with hash/number: ${nextBlock}: ${error}`);
      } else if (blockData === null) {
        console.log(`Warning: null block data received from the block with hash/number: ${nextBlock}`);
      } else {
        writeBlockToDB(config_, blockData);
        writeTransactionsToDB(config_, blockData);
        ++done;
      }
    }

    let count = config_.bulkSize;
    console.log("new round for block " + nextBlock);
    while (nextBlock >= config_.startBlock && count > 0) {
      web3.eth.getBlock(nextBlock, true, processBlock);
      nextBlock--;
      count--;
    }

    console.log('waiting batch finish for next block' + nextBlock);
    // block
    var nextClock = setInterval(() => {
      if (done != config_.bulkSize) return;
      syncChain(config_, nextBlock);
      clearInterval(nextClock);
    }, 1000);

  } else {
    console.log(`Error: Web3 connection time out trying to get block ${nextBlock} retrying connection now`);
    syncChain(config_, nextBlock);
  }
};

/**
  //check oldest block or starting block then callback
**/
const prepareSync = async (config_, callback) => {
  let blockNumber = null;
  const oldBlockFind = Block.find({}, 'number').lean(true).sort('number').limit(1);
  oldBlockFind.exec(async (err, docs) => {
    if (err || !docs || docs.length < 1) {
      // not found in db. sync from config.endBlock or 'latest'
      if (web3.eth.net.isListening()) {
        const currentBlock = await web3.eth.getBlockNumber();
        const latestBlock = config_.endBlock || currentBlock || 'latest';
        if (latestBlock === 'latest') {
          web3.eth.getBlock(latestBlock, true, (error, blockData) => {
            if (error) {
              console.log(`Warning (prepareSync): error on getting block with hash/number: ${latestBlock}: ${error}`);
            } else if (blockData === null) {
              console.log(`Warning: null block data received from the block with hash/number: ${latestBlock}`);
            } else {
              console.log(`Starting block number = ${blockData.number}`);
              if ('quiet' in config_ && config_.quiet === true) {
                console.log('Quiet mode enabled');
              }
              blockNumber = blockData.number - 1;
              callback(null, blockNumber);
            }
          });
        } else {
          console.log(`Starting block number = ${latestBlock}`);
          if ('quiet' in config_ && config_.quiet === true) {
            console.log('Quiet mode enabled');
          }
          blockNumber = latestBlock - 1;
          callback(null, blockNumber);
        }
      } else {
        console.log('Error: Web3 connection error');
        callback(err, null);
      }
    } else {
      blockNumber = docs[0].number - 1;
      console.log(`Old block found. Starting block number = ${blockNumber}`);
      if ('quiet' in config_ && config_.quiet === true) {
        console.log('Quiet mode enabled');
      }
      callback(null, blockNumber);
    }
  });
};

/**
  Block Patcher(experimental)
**/
const runPatcher = async (config_, startBlock, endBlock) => {
  if (!web3 || !web3.eth.net.isListening()) {
    console.log('Error: Web3 is not connected. Retrying connection shortly...');
    setTimeout(() => { runPatcher(config_); }, 3000);
    return;
  }

  if (typeof startBlock === 'undefined' || typeof endBlock === 'undefined') {
    // get the last saved block
    const blockFind = Block.find({}, 'number').lean(true).sort('-number').limit(1);
    blockFind.exec(async (err, docs) => {
      if (err || !docs || docs.length < 1) {
        // no blocks found. terminate runPatcher()
        console.log('No need to patch blocks.');
        return;
      }

      const lastMissingBlock = docs[0].number + 1;
      const currentBlock = await web3.eth.getBlockNumber();
      runPatcher(config_, lastMissingBlock, currentBlock - 1);
    });
    return;
  }

  const missingBlocks = endBlock - startBlock + 1;
  if (missingBlocks > 0) {
    if (!('quiet' in config_ && config_.quiet === true)) {
      console.log(`Patching from #${startBlock} to #${endBlock}`);
    }
    let patchBlock = startBlock;
    let count = 0;
    while (count < config_.patchBlocks && patchBlock <= endBlock) {
      if (!('quiet' in config_ && config_.quiet === true)) {
        console.log(`Patching Block: ${patchBlock}`);
      }
      web3.eth.getBlock(patchBlock, true, (error, patchData) => {
        if (error) {
          console.log(`Warning: error on getting block with hash/number: ${patchBlock}: ${error}`);
        } else if (patchData === null) {
          console.log(`Warning: null block data received from the block with hash/number: ${patchBlock}`);
        } else {
          checkBlockDBExistsThenWrite(config_, patchData);
        }
      });
      patchBlock++;
      count++;
    }
    // flush
    writeBlockToDB(config_, null, true);
    writeTransactionsToDB(config_, null, true);

    setTimeout(() => { runPatcher(config_, patchBlock, endBlock); }, 1000);
  } else {
    // flush
    writeBlockToDB(config_, null, true);
    writeTransactionsToDB(config_, null, true);

    console.log('*** Block Patching Completed ***');
  }
};

/**
  This will be used for the patcher(experimental)
**/
const checkBlockDBExistsThenWrite = function (config_, patchData, flush) {
  Block.find({ number: patchData.number }, (err, b) => {
    if (!b.length) {
      writeBlockToDB(config_, patchData, flush);
      writeTransactionsToDB(config_, patchData, flush);
    } else if (!('quiet' in config_ && config_.quiet === true)) {
      console.log(`Block number: ${patchData.number.toString()} already exists in DB.`);
    }
  });
};

/**
  Fetch market price from cryptocompare
**/
// 10 minutes
const quoteInterval = 10 * 60 * 1000;

const getQuote = async () => {
  const URL = `https://min-api.cryptocompare.com/data/price?fsym=${config.settings.symbol}&tsyms=USD`;

  try {
    const requestUSD = await fetch(URL);
    const quoteUSD = await requestUSD.json();

    var quoteObject = {
      timestamp: Math.round(Date.now() / 1000),
      quoteUSD: quoteUSD.USD,
    };

    new Market(quoteObject).save((err, market, count) => {
      if (typeof err !== 'undefined' && err) {
        process.exit(9);
      } else {
        if (!('quiet' in config && config.quiet === true)) {
          console.log('DB successfully written for market quote.');
        }
      }
    });
  } catch (error) {
    if (!('quiet' in config && config.quiet === true)) {
      console.log(error);
    }
  }
};

// patch missing blocks
if (config.patch === true) {
  console.log('Checking for missing blocks');
  runPatcher(config);
}

// check NORICHLIST env
// you can use it like as 'NORICHLIST=1 node tools/sync.js' to disable balance updater temporary.
if (process.env.NORICHLIST) {
  config.settings.useRichList = false;
}

// Start listening for latest blocks
listenBlocks(config);

// Starts full sync when set to true in config
if (config.syncAll === true) {
  console.log('Starting Full Sync');
  syncChain(config);
}

// Start price sync on DB
if (config.settings.useFiat) {
  getQuote();

  setInterval(() => {
    getQuote();
  }, quoteInterval);
}
