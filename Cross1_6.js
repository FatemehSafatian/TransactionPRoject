require("dotenv").config();
const { Alchemy, Network } = require("alchemy-sdk");
const { Pool } = require("pg");
const axios = require("axios");
const { v4: uuidv4 } = require("uuid");

const tokenContracts = require("./tokenContracts.json");
const settingsArbitrum = {
  apiKey: process.env.API_KEY_ALCHEMY,
  network: Network.ARB_MAINNET,
};
const alchemyInstanceArbitrum = new Alchemy(settingsArbitrum);

const pool = new Pool({
  user: process.env.USER,
  host: process.env.HOST,
  database: process.env.DATABASE,
  password: process.env.PASSWORD,
  port: process.env.PORT,
});

const correlationMap = new Map();
const inboundActions = [
  "deposit",
  "depositNative",
  "depositNativeToken",
  "depositWeiIntoDefaultAccount",
  "depositETHIntoDefaultAccount",
  "depositTransaction",
  "depositETH",
  "depositCollateral",
  "addLiquidity",
  "bridgeIn",
  "finalizeInboundTransfer",
  "receiveMessage",
  "mint",
  "receiveFunds",
  "receiveTokens",
  "acceptDeposit",
  "approveDeposit",
  "handleDeposit",
  "handleInboundTransfer",
  "receiveCrossChain",
  "finalizeDeposit",
  "initiateDeposit",
  "requestL2Transaction",
];
const outboundActions = [
  "WithdrawalInitiated",
  "initiateWithdrawal",
  "withdraw",
  "withdrawRewards",
  "withdrawAll",
  "withdrawBySig",
  "bondWithdrawalAndDistribute",
  "transferOut",
  "unwrap",
  "send",
  "sendFrom",
  "sendTokens",
  "redeem",
  "redeemTokens",
  "redeemDueInterestAndRewards",
  "redeemRewards",
  "redeemFees",
  "placeWithdrawalOrder",
  "releaseFunds",
  "releaseTokens",
  "finalizeWithdrawal",
  "handleWithdrawal",
  "transferCrossChain",
  "exit",
  "exitFunds",
  "exitTokens",
  "crossChainTransfer",
  "outboundTransfer",
  "finalizeWithdrawal",
  "swapAndStartBridgeTokensViaHyphen",
  "bridge",
];

async function getLastBlockNumber() {
  try {
    const queryText =
      "SELECT block_number FROM L2_Transactions ORDER BY id DESC LIMIT 1;";
    const res = await pool.query(queryText);
    return res.rows.length > 0 ? res.rows[0].block_number : null;
  } catch (err) {
    console.error(`Error fetching last block number :`, err);
    throw err;
  }
}

async function saveBlock(block) {
  try {
    const blockInsertText =
      "INSERT INTO L2_blocks(block_number, timestamp) VALUES($1, $2) ON CONFLICT (block_number) DO NOTHING;";
    await pool.query(blockInsertText, [
      block.number,
      new Date(block.timestamp * 1000),
    ]);
  } catch (error) {
    console.error(`Error saving block block ${block}: `, error);
  }
}

async function fetchAndStoreBlockData(blockNumber) {
  try {
    const block = await alchemyInstanceArbitrum.core.getBlockWithTransactions(
      blockNumber
    );

    if (!block || !block.transactions || block.transactions.length === 0) {
      console.log(`No transactions found block ${blockNumber}`);
      return false;
    }

    const transactionDetailsPromises = block.transactions.map(async (tx) => {
      return await fetchTransactionDetails(tx, block.timestamp);
    });

    const transactionDetails = (
      await Promise.all(transactionDetailsPromises)
    ).filter((details) => details != null);

    if (transactionDetails.length > 0) {
      const blockExists = await pool.query(
        "SELECT 1 FROM L2_blocks WHERE block_number = $1 ",
        [blockNumber]
      );

      if (blockExists.rows.length === 0) {
        await saveBlock(block);
      }

      await batchStoreTransactions(transactionDetails);
      return true;
    }
    return false;
  } catch (error) {
    console.error(`Error fetching transactions block ${blockNumber}: `, error);
    return false;
  }
}

async function batchStoreTransactions(transactions) {
  const queryParams = [];
  const values = transactions
    .map((tx, index) => {
      const baseIndex = index * 13 + 1;
      queryParams.push(
        `($${baseIndex}, $${baseIndex + 1}, $${baseIndex + 2}, $${
          baseIndex + 3
        }, $${baseIndex + 4}, $${baseIndex + 5}, $${baseIndex + 6}, $${
          baseIndex + 7
        }, $${baseIndex + 8}, $${baseIndex + 9}, $${baseIndex + 10}, $${
          baseIndex + 11
        }, $${baseIndex + 12})`
      );
      return [
        tx.hash,
        tx.blockNumber,
        tx.from,
        tx.to,
        tx.value,
        tx.input,
        tx.abiMethodId,
        tx.contractType,
        tx.direction,
        tx.timestamp,
        tx.gasFee,
        tx.gasPricePaid,
        tx.correlationId,
      ];
    })
    .flat();

  const query = `
    INSERT INTO L2_transactions
    (tx_hash, block_number, from_address, to_address, value, input, method_abi_id, transaction_type, tx_direction, timestamp, gas_fee, gas_price_paid, correlationId)
    VALUES ${queryParams.join(", ")}
    ON CONFLICT (tx_hash, from_address, to_address, method_abi_id, block_number) DO UPDATE
    SET value = EXCLUDED.value,
        input = EXCLUDED.input,
        transaction_type = EXCLUDED.transaction_type,
        tx_direction = EXCLUDED.tx_direction,
        timestamp = EXCLUDED.timestamp,
        gas_fee = EXCLUDED.gas_fee,
        gas_price_paid = EXCLUDED.gas_price_paid,
        correlationId = EXCLUDED.correlationId;
`;

  try {
    const result = await pool.query(query, values);
    if (result.rowCount > 0) {
      console.log(`Successfully saved ${result.rowCount} transactions.`);
    } else {
      console.log("No transactions were saved.");
    }
  } catch (error) {
    console.error("Failed to batch store transactions: ", error);
  }
}

async function isRelevantTransaction(fromAddress, toAddress, methodSignature) {
  let dir = "";
  if (methodSignature) {
    if (
      inboundActions.some((action) =>
        methodSignature.includes(action.toLowerCase())
      )
    ) {
      dir = "Inbound (External to Arbitrum)";
    } else if (
      outboundActions.some((action) =>
        methodSignature.includes(action.toLowerCase())
      )
    ) {
      dir = "Outbound (Arbitrum to External)";
    }
  }
  if (tokenContracts[toAddress] || tokenContracts[fromAddress] || dir != "")
    return dir;

  return null;
}

async function fetchTransactionDetails(tx, timestamp) {
  let direction;
  let correlationId;
  let gasFee = 0;
  let gasPricePaid = 0;

  try {
    const fromAddress = tx.from ? tx.from.toLowerCase() : null;
    const toAddress = tx.to ? tx.to.toLowerCase() : null;

    if (tx.to === null && tx.creates) {
      return null;
    }

    const methodABIId = await getOrStoreABI(tx.data.slice(0, 10));
    if (methodABIId == "unknown") return null;

    let contractType =
      (toAddress && tokenContracts[toAddress]?.type) ||
      (fromAddress && tokenContracts[fromAddress]?.type) ||
      "unknown";

    const methodSignature = await getAbiMethodName(methodABIId);

    const relevantCheck = await isRelevantTransaction(
      fromAddress,
      toAddress,
      methodSignature
    );

    if (relevantCheck == null) return null;
    if (relevantCheck != "") direction = relevantCheck;
    else direction = await determineDirection(fromAddress, toAddress);

    if (tx.gasPrice && tx.gasLimit) {
      gasFee = parseFloat(tx.gasPrice) * parseFloat(tx.gasLimit);
    }

    if (tx.gasPrice) {
      gasPricePaid = fromWei(tx.gasPrice);
    }
    if (correlationMap.has(tx.hash))
      correlationId = correlationMap.get(tx.hash);
    else if (tx.to && correlationMap.has(tx.to.toLowerCase())) {
      correlationId = correlationMap.get(tx.to.toLowerCase());
    } else if (tx.from && correlationMap.has(tx.from.toLowerCase())) {
      correlationId = correlationMap.get(tx.from.toLowerCase());
    } else {
      correlationId = uuidv4();
    }
    correlationMap.set(tx.hash.toLowerCase(), correlationId);
    correlationMap.set(toAddress, correlationId);
    correlationMap.set(fromAddress, correlationId);

    return {
      hash: tx.hash,
      blockNumber: tx.blockNumber,
      from: fromAddress,
      to: toAddress,
      value: tx.value ? fromWei(tx.value.toString()) : "0",
      input: tx.data,
      abiMethodId: methodABIId,
      contractType: contractType,
      direction: direction,
      timestamp: new Date(timestamp * 1000),
      gasFee: fromWei(gasFee.toString()),
      gasPricePaid: gasPricePaid,
      correlationId: correlationId,
    };
  } catch (error) {
    console.error(`Error in fetchTransactionDetails for tx ${tx.hash}:`, error);
    return null;
  }
}

async function getOrStoreABI(methodId) {
  try {
    const abiCheckText =
      "SELECT id FROM abis WHERE method_id = $1 AND Method_signature <> 'Unknown method';";
    const res = await pool.query(abiCheckText, [methodId]);
    if (res.rows.length > 0) {
      return res.rows[0].id;
    } else {
      let methodSignature = "Unknown method";
      if (methodId != "0x")
        methodSignature = await fetchMethodSignature(methodId);

      if (methodSignature === "Unknown method") {
        return "unknown";
      }

      let methodKeyword = methodSignature.split("(")[0].toLowerCase().trim();

      const abiInsertText =
        "INSERT INTO abis(method_id, method_signature) VALUES($1, $2) RETURNING id;";
      const insertRes = await pool.query(abiInsertText, [
        methodId,
        methodKeyword,
      ]);
      return insertRes.rows[0].id;
    }
  } catch (error) {
    console.error("Error fetching method signature !!:", error);
    return "unknown";
  }
}

async function fetchMethodSignature(methodId, retries = 3) {
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  for (let i = 0; i <= retries; i++) {
    try {
      const response = await axios.get(
        `https://www.4byte.directory/api/v1/signatures/?hex_signature=${methodId}`
      );

      if (response && response.data.count > 0) {
        return response.data.results[0].text_signature;
      } else {
        return "Unknown method";
      }
    } catch (error) {
      if (error.response && error.response.status === 429 && i < retries) {
        const retryAfterHeader = error.response.headers["retry-after"];
        let retryAfter = 0;

        if (retryAfterHeader) {
          if (isNaN(retryAfterHeader)) {
            retryAfter =
              new Date(retryAfterHeader).getTime() - new Date().getTime();
          } else {
            retryAfter = parseInt(retryAfterHeader) * 2000;
          }
        } else {
          retryAfter = Math.pow(2, i) * 2000;
        }

        console.error(`Rate limited. Retrying after ${retryAfter} ms...`);
        await delay(retryAfter);
      } else {
        console.error("Error fetching method signature:", error.message);
        return "Unknown method";
      }
    }
  }

  return "Unknown method";
}

async function determineDirection(fromAddress, toAddress) {
  const fromDirectionInfo = tokenContracts[fromAddress]?.direction;
  const toDirectionInfo = tokenContracts[toAddress]?.direction;

  try {
    if (
      toDirectionInfo &&
      (toDirectionInfo === "ingress" || toDirectionInfo === "both")
    ) {
      return "Inbound (External to Arbitrum)";
    } else if (
      fromDirectionInfo &&
      (fromDirectionInfo === "egress" || fromDirectionInfo === "both")
    ) {
      return "Outbound (Arbitrum to External)";
    }

    return "Internal";
  } catch (error) {
    console.error(`Error determining direction: ${error}`);
    throw error;
  }
}

async function getAbiMethodName(methodABIId) {
  const abiCheckText = "SELECT method_signature FROM abis WHERE id = $1;";
  const res = await pool.query(abiCheckText, [methodABIId]);
  const methodSignature =
    res.rows.length > 0
      ? res.rows[0].method_signature.toLowerCase().trim()
      : "";
  return methodSignature;
}

function fromWei(value) {
  return parseFloat(value) / 1e18;
}

async function syncBlockchainData(startBlock, endBlock) {
  for (let blockNumber = startBlock; blockNumber <= endBlock; blockNumber++) {
    await fetchAndStoreBlockData(blockNumber);
  }
}

async function main() {
  try {
    console.log("Fetching last block numbers...");
    const lastArbitrumBlock = parseInt(await getLastBlockNumber());

    console.log("Fetching last block numbers...");
    const latestArbitrum = await alchemyInstanceArbitrum.core.getBlockNumber();

    const startArbitrumBlock = lastArbitrumBlock
      ? lastArbitrumBlock + 1
      : latestArbitrum - 50000;

    let last_item = startArbitrumBlock + 50000;

    await syncBlockchainData(startArbitrumBlock, last_item);

    console.log("Main function execution completed.");
  } catch (error) {
    console.error("Error in main function:", error);
  }
}

main().catch(console.error);
