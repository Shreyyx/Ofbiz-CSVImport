package com.companyname.ofbizdemo.services;

import org.apache.ofbiz.base.util.Debug;
import org.apache.ofbiz.base.util.UtilDateTime;
import org.apache.ofbiz.base.util.UtilMisc;
import org.apache.ofbiz.base.util.UtilValidate;
import org.apache.ofbiz.entity.Delegator;
import org.apache.ofbiz.entity.GenericValue;
import org.apache.ofbiz.entity.transaction.GenericTransactionException;
import org.apache.ofbiz.entity.transaction.TransactionUtil;
import org.apache.ofbiz.service.DispatchContext;
import org.apache.ofbiz.service.ServiceUtil;
import org.apache.ofbiz.entity.GenericEntityException;
import java.util.Date;
import java.sql.Timestamp;
import java.util.TimeZone;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Map;
import java.util.List;

public class ProductCsv {
    public static final String module = ProductCsv.class.getName();

    public static Map<String, Object> productCsv(DispatchContext dctx, Map<String, ?> context) {
        Debug.logInfo("Starting CSV import process", module);
        boolean beganTransaction = false;
        String csvFilePath = (String) context.get("csvFilePath");
        Delegator delegator = dctx.getDelegator();
        Map<String, Object> result = ServiceUtil.returnSuccess();

        try {
            beganTransaction = TransactionUtil.begin();
            Debug.logInfo("Transaction started", module);

            BufferedReader reader = new BufferedReader(new FileReader(csvFilePath, StandardCharsets.UTF_8));
            String line;
            boolean isHeader = true;
            int lineNumber = 0;

            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (isHeader) {
                    isHeader = false;
                    continue;
                }

                String[] fields = line.split(",");

                String productId = fields[0].trim();
                String productTypeId = fields[1].trim();
                String internalName = fields[2].trim();
                String prodCatalogId = fields[3].trim();
                String catalogName = fields[4].trim();
                String productCategoryId = fields[5].trim();
                String productCategoryTypeId = fields[6].trim();
                String primaryParentCategoryId = fields[7].trim();
                String categoryName = fields[8].trim();
                String description = fields[9].trim();
                String prodCatalogCategoryTypeId = fields[10].trim();
//                String fromDate = fields[11].trim();
                String productFeatureId = fields[11].trim();
                String productFeatureTypeId = fields[12].trim();
                String productPriceTypeId = fields[13].trim();
                String productPricePurposeId = fields[14].trim();
                String currencyUomId = fields[15].trim();
                String productStoreGroupId = fields[16].trim();
                String price = fields[17].trim();
                String productFeatureApplTypeId = fields[18].trim();
                String productStoreId = fields[19].trim();

                Debug.logInfo("============================Product ID: " + productId + "============================", module);

                if (UtilValidate.isEmpty(productId) || UtilValidate.isEmpty(productTypeId) || UtilValidate.isEmpty(internalName)) {
                    Debug.logWarning("Skipping invalid row (missing required fields) at line " + lineNumber + ": " + line, module);
                    continue;
                }

                Debug.logInfo("Parsed CSV Line: " + line, module);
                Debug.logInfo("Product ID: " + productId, module);

                try {
                    GenericValue product = delegator.findOne("Product", UtilMisc.toMap("productId", productId), false);
                    if (product == null) {
                        product = delegator.makeValue("Product");
                        product.set("productId", productId);
                        product.set("productTypeId", productTypeId);
                        product.set("internalName", internalName);
                        product.set("isVirtual", "N");
                        product.set("isVariant", "Y");
                        delegator.create(product);

                    } else {
                        product.set("productTypeId", productTypeId);
                        product.set("internalName", internalName);
                        product.set("isVirtual", "N");
                        product.set("isVariant", "Y");
                        delegator.store(product);
                    }

                } catch (Exception e) {
                    Debug.logError(e, "Error creating/storing Product at line " + lineNumber + ": " + e.getMessage(), module);
                    continue;
                }

                try {
                    GenericValue prodCatalog = delegator.findOne("ProdCatalog", UtilMisc.toMap("prodCatalogId", prodCatalogId), false);

                    if (prodCatalog == null) {
                        prodCatalog = delegator.makeValue("ProdCatalog");
                        prodCatalog.set("prodCatalogId", prodCatalogId);
                        prodCatalog.set("catalogName", catalogName);
                        delegator.create(prodCatalog);
                    } else {
                        prodCatalog.set("catalogName", catalogName);
                        delegator.store(prodCatalog);
                    }

                } catch (Exception e) {
                    Debug.logError(e, "Error creating/storing Product Catalog at line " + lineNumber + ": " + e.getMessage(), module);
                    continue;
                }

                try {
                    GenericValue productCategory = delegator.findOne("ProductCategory", UtilMisc.toMap("productCategoryId", productCategoryId), false);

                    if (productCategory == null) {
                        productCategory = delegator.makeValue("ProductCategory");
                        productCategory.set("productCategoryId", productCategoryId);
                        productCategory.set("productCategoryTypeId", productCategoryTypeId);
                        productCategory.set("primaryParentCategoryId", primaryParentCategoryId);
                        productCategory.set("categoryName", categoryName);
                        productCategory.set("description", description);
                        delegator.create(productCategory);
                    } else {
                        productCategory.set("productCategoryTypeId", productCategoryTypeId);
                        productCategory.set("primaryParentCategoryId", primaryParentCategoryId);
                        productCategory.set("categoryName", categoryName);
                        productCategory.set("description", description);
                        delegator.store(productCategory);
                    }
                } catch (Exception e) {
                    Debug.logError(e, "Error creating/storing Product Category at line " + lineNumber + ": " + e.getMessage(), module);
                }

                try {
                    GenericValue productFeature = delegator.findOne("ProductFeature", UtilMisc.toMap("productFeatureId", productFeatureId), false);

                    if (productFeature == null) {
                        productFeature = delegator.makeValue("ProductFeature");
                        productFeature.set("productFeatureId", productFeatureId);
                        productFeature.set("productFeatureTypeId", productFeatureTypeId);
                        delegator.create(productFeature);
                    } else {
                        productFeature.set("productFeatureTypeId", productFeatureTypeId);
                        delegator.store(productFeature);
                    }
                } catch (Exception e) {
                    Debug.logError(e, "Error creating/storing Product Feature at line " + lineNumber + ": " + e.getMessage(), module);
                }

                try {
                    GenericValue productPrice = delegator.findOne("ProductPrice", UtilMisc.toMap("productId", productId,"productPriceTypeId", productPriceTypeId, "productPricePurposeId", productPricePurposeId, "currencyUomId", currencyUomId, "productStoreGroupId", productStoreGroupId,"fromDate", UtilDateTime.nowTimestamp()), false);


                    if (productPrice == null) {
                        // Create new ProductPrice entry
                        productPrice = delegator.makeValue("ProductPrice");
                        productPrice.set("productId", productId);
                        productPrice.set("productPriceTypeId", productPriceTypeId);
                        productPrice.set("productPricePurposeId", productPricePurposeId);
                        productPrice.set("currencyUomId", currencyUomId);
                        productPrice.set("price", new BigDecimal(price));
                        productPrice.set("productStoreGroupId", productStoreGroupId);
                        productPrice.set("fromDate", UtilDateTime.nowTimestamp());
                        delegator.create(productPrice);
                    } else {
                        // Update existing ProductPrice entry
                        productPrice.set("price", new BigDecimal(price));
                        productPrice.set("fromDate", UtilDateTime.nowTimestamp());
                        delegator.store(productPrice);
                    }
                } catch (Exception e) {
                    Debug.logError(e, "Error creating/storing ProductPrice at line " + lineNumber + ": " + e.getMessage(), module);
                }

                try {
                    GenericValue prodCatalogCategory = delegator.findOne("ProdCatalogCategory", UtilMisc.toMap("prodCatalogId", prodCatalogId, "productCategoryId", productCategoryId, "prodCatalogCategoryTypeId", prodCatalogCategoryTypeId, "fromDate", UtilDateTime.nowTimestamp()), false);

                    if (prodCatalogCategory == null) {
                        prodCatalogCategory = delegator.makeValue("ProdCatalogCategory");
                        prodCatalogCategory.set("prodCatalogId", prodCatalogId);
                        prodCatalogCategory.set("productCategoryId", productCategoryId);
                        prodCatalogCategory.set("prodCatalogCategoryTypeId", prodCatalogCategoryTypeId);
                        prodCatalogCategory.set("fromDate", UtilDateTime.nowTimestamp());
                        delegator.create(prodCatalogCategory);
                    } else {
                        prodCatalogCategory.set("prodCatalogCategoryTypeId", prodCatalogCategoryTypeId);
                        prodCatalogCategory.set("fromDate", UtilDateTime.nowTimestamp());
                        delegator.store(prodCatalogCategory);
                    }
                } catch (Exception e) {
                    Debug.logError(e, "Error creating/storing Product Catalog Category at line" + lineNumber + ": " + e.getMessage(), module);
                }

                try{
                    GenericValue productFeatureAppl = delegator.findOne("ProductFeatureAppl",
                        UtilMisc.toMap("productId", productId, "productFeatureId", productFeatureId, "fromDate", UtilDateTime.nowTimestamp()),
                        false);

                        productFeatureAppl = delegator.makeValue("ProductFeatureAppl");
                        productFeatureAppl.set("productId", productId);
                        productFeatureAppl.set("productFeatureId", productFeatureId);
                        productFeatureAppl.set("fromDate", UtilDateTime.nowTimestamp()) ;
                        productFeatureAppl.set("productFeatureApplTypeId", productFeatureApplTypeId);
                        delegator.create(productFeatureAppl);

                } catch (Exception e) {
                    Debug.logError(e, "Error creating/storing Product Feature Applicability at line " + lineNumber + ": " + e.getMessage(), module);
                }

                try {
                    GenericValue productStoreCatalog = delegator.findOne("ProductStoreCatalog", UtilMisc.toMap("productStoreId", productStoreId, "prodCatalogId", prodCatalogId, "fromDate", UtilDateTime.nowTimestamp()), false);
                    if (productStoreCatalog == null) {
                        productStoreCatalog = delegator.makeValue("ProductStoreCatalog");
                        productStoreCatalog.set("productStoreId", productStoreId);
                        productStoreCatalog.set("prodCatalogId", prodCatalogId);
                        productStoreCatalog.set("fromDate", UtilDateTime.nowTimestamp());
                        delegator.create(productStoreCatalog);
                    }
                    else{
                        productStoreCatalog.set("fromDate", UtilDateTime.nowTimestamp());
                        delegator.store(productStoreCatalog);
                        Debug.logInfo("Updated existing ProductStoreCatalog for productId: " + productId, module);
                    }


                } catch (Exception e) {
                    Debug.logError(e, "Error creating/storing Product Store Catalog at line " + lineNumber + ": " + e.getMessage(), module);
                }

                try {
                    GenericValue productCategoryMember = delegator.findOne("ProductCategoryMember",
                            UtilMisc.toMap("productCategoryId", productCategoryId, "productId", productId, "fromDate", UtilDateTime.nowTimestamp()),
                            false);

                    if (productCategoryMember == null) {
                        productCategoryMember = delegator.makeValue("ProductCategoryMember");
                        productCategoryMember.set("productCategoryId", productCategoryId);
                        productCategoryMember.set("productId", productId);
                        productCategoryMember.set("fromDate", UtilDateTime.nowTimestamp());
                        delegator.create(productCategoryMember);

                    } else {
                        productCategoryMember.set("fromDate", UtilDateTime.nowTimestamp());
                        delegator.store(productCategoryMember);
                        Debug.logInfo("Updated existing ProductCategoryMember for productId: " + productId, module);
                    }
                } catch (Exception e) {
                    Debug.logError(e, "Error creating/storing Product Category Member at line " + lineNumber + ": " + e.getMessage(), module);
                }


                Debug.logInfo("Successfully imported product: " + productId, module);
            }

            reader.close();
            TransactionUtil.commit(beganTransaction);
            Debug.logInfo("Transaction committed", module);

        } catch (IOException e) {
            Debug.logError(e, "Error reading CSV file", module);
            if (beganTransaction) {
                try {
                    TransactionUtil.rollback(beganTransaction, "Error in CSV Import: " + e.getMessage(), e);
                } catch (GenericTransactionException gte) {
                    Debug.logError(gte, "Error rolling back transaction", module);
                }
            }
            return ServiceUtil.returnError("CSV Import Failed: " + e.getMessage());

        } catch (Exception e) {
            if (beganTransaction) {
                try {
                    TransactionUtil.rollback(beganTransaction, "Error in CSV Import", e);
                    Debug.logError("Transaction Rolled Back: " + e.getMessage(), module);
                } catch (GenericTransactionException gte) {
                    Debug.logError(gte, "Error rolling back transaction", module);
                }
            }
            Debug.logError(e, "Error processing CSV file", module);
            return ServiceUtil.returnError("CSV Import Failed: " + e.getMessage());
        }

        return result;
    }
}


