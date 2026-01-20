import dotenv from "dotenv";
dotenv.config();

const shopifyConfig = {
  apiKey: process.env.SHOPIFY_API_KEY,
  apiSecretKey: process.env.SHOPIFY_API_SECRET,
  scopes: ["read_products", "write_products"],
  hostName: "localhost",
};

export default shopifyConfig;