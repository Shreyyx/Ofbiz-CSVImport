package com.companyname.ofbizdemo.services;

import org.apache.ofbiz.base.util.Debug;
import org.apache.ofbiz.base.util.UtilMisc;
import org.apache.ofbiz.base.util.UtilValidate;
import org.apache.ofbiz.entity.Delegator;
import org.apache.ofbiz.entity.GenericValue;
import org.apache.ofbiz.entity.transaction.TransactionUtil;
import org.apache.ofbiz.entity.transaction.GenericTransactionException;
import org.apache.ofbiz.service.DispatchContext;
import org.apache.ofbiz.service.ServiceUtil;
import org.apache.ofbiz.service.LocalDispatcher;
import org.apache.ofbiz.service.GenericServiceException;
import org.apache.ofbiz.entity.util.EntityQuery;
import org.apache.ofbiz.base.util.UtilDateTime;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ImportProductsFromCsv {
    public static final String module = ImportProductsFromCsv.class.getName();

    public static Map<String, Object> ImportProductsFromCsv(DispatchContext dctx, Map<String, ?> context) {
        Debug.logInfo("Starting CSV import process", module);

        boolean beganTransaction = false;
        String csvFilePath = (String) context.get("csvFilePath");
        Delegator delegator = dctx.getDelegator();
        LocalDispatcher dispatcher = dctx.getDispatcher();
        Map<String, Object> result = ServiceUtil.returnSuccess();

        try {
            beganTransaction = TransactionUtil.begin();
            Debug.logInfo("Transaction started", module);

            BufferedReader reader = new BufferedReader(new FileReader(csvFilePath, StandardCharsets.UTF_8));
            String line;
            boolean isHeader = true;
            Map<String, Integer> headerIndexMap = null;

            GenericValue permUserLogin = EntityQuery.use(delegator)
                    .from("UserLogin")
                    .where("userLoginId", "system")
                    .queryOne();

            if (permUserLogin == null) {
                Debug.logError("System userLogin not found!", module);
                return ServiceUtil.returnError("System userLogin not found!");
            }

            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");

                if (isHeader) {
                    headerIndexMap = getColumnIndexMap(fields);
                    isHeader = false;
                    continue;
                }
                Map<String, Object> serviceContext = new HashMap<>();
                String productId = getValue(fields, headerIndexMap, "productId");
                String productTypeId = getValue(fields, headerIndexMap, "productTypeId");
                String internalName = getValue(fields, headerIndexMap, "internalName");
                String prodCatalogId = getValue(fields, headerIndexMap, "prodCatalogId");
                String catalogName = getValue(fields, headerIndexMap, "catalogName");
                String productCategoryId = getValue(fields, headerIndexMap, "productCategoryId");
                String productCategoryTypeId = getValue(fields, headerIndexMap, "productCategoryTypeId");
                String primaryParentCategoryId = getValue(fields, headerIndexMap, "primaryParentCategoryId");
                String categoryName = getValue(fields, headerIndexMap, "categoryName");
                String description = getValue(fields, headerIndexMap, "description");
                String prodCatalogCategoryTypeId = getValue(fields, headerIndexMap, "prodCatalogCategoryTypeId");
                String productFeatureId = getValue(fields, headerIndexMap, "productFeatureId");
                String productFeatureTypeId = getValue(fields, headerIndexMap, "productFeatureTypeId");
                String productPriceTypeId = getValue(fields, headerIndexMap, "productPriceTypeId");
                String productPricePurposeId = getValue(fields, headerIndexMap, "productPricePurposeId");
                String currencyUomId = getValue(fields, headerIndexMap, "currencyUomId");
                String price = getValue(fields, headerIndexMap, "price");
                String fromDateStr = getValue(fields, headerIndexMap, "fromDate");
                String productStoreGroupId = getValue(fields, headerIndexMap, "productStoreGroupId");
                String productFeatureApplTypeId = getValue(fields, headerIndexMap, "productFeatureApplTypeId");
                String productStoreId = getValue(fields, headerIndexMap, "productStoreId");

                Timestamp fromDate = null;

                if (fromDateStr != null && !fromDateStr.isEmpty()) {
                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        fromDate = new Timestamp(sdf.parse(fromDateStr).getTime());
                    } catch (Exception e) {
                        Debug.logError("Invalid date format for fromDate: " + fromDateStr, module);
                        return ServiceUtil.returnError("Invalid date format for fromDate. Expected: yyyy-MM-dd HH:mm:ss");
                    }
                }

                if (UtilValidate.isEmpty(productId)) {
                    Debug.logError("ProductId is required and cannot be empty.", module);
                    return ServiceUtil.returnError("ProductId is required and cannot be empty.");
                }
                try {
                    Debug.logInfo("Checking if Product with ID: " + productId + " exists", module);

                    GenericValue existingProduct = EntityQuery.use(delegator)
                            .from("Product")
                            .where("productId", productId)
                            .queryOne();

                    if (existingProduct != null) {
                        Debug.logInfo("Product with ID: " + productId + " already exists. Updating instead of creating.", module);

                        existingProduct.set("productTypeId", productTypeId);
                        existingProduct.set("internalName", internalName);
                        existingProduct.store();

                        Debug.logInfo("Product with ID: " + productId + " updated successfully.", module);
                    } else {
                        Debug.logInfo("Creating new Product with ID: " + productId, module);
                        dispatcher.runSync("createProduct", UtilMisc.toMap(
                                "productId", productId,
                                "productTypeId", productTypeId,
                                "internalName", internalName,
                                "userLogin", permUserLogin
                        ));
                    }
                } catch (GenericServiceException e) {
                    Debug.logError("Unable to create/update product: " + e.getMessage(), module);
                    try {
                        TransactionUtil.rollback(beganTransaction, "Error creating/updating product", e);
                    } catch (GenericTransactionException rollbackException) {
                        Debug.logError("Transaction rollback failed: " + rollbackException.getMessage(), module);
                    }
                    return ServiceUtil.returnError("Error creating/updating product: " + e.getMessage());
                }
                try {
                    Debug.logInfo("Checking if Product Catalog exists with ID: " + prodCatalogId, module);
                    GenericValue existingProdCatalog = EntityQuery.use(delegator)
                            .from("ProdCatalog")
                            .where("prodCatalogId", prodCatalogId)
                            .queryOne();

                    if (existingProdCatalog == null) {
                        Debug.logInfo("Creating new Product Catalog with ID: " + prodCatalogId, module);
                        dispatcher.runSync("createProdCatalog", UtilMisc.toMap(
                                "prodCatalogId", prodCatalogId,
                                "catalogName", catalogName,
                                "userLogin", permUserLogin
                        ));
                    } else {
                        Debug.logInfo("Updating existing Product Catalog with ID: " + prodCatalogId, module);
                        existingProdCatalog.setString("catalogName", catalogName);
                        existingProdCatalog.store();
                    }
                } catch (GenericServiceException e) {
                    Debug.logError("Error handling product catalog: " + e.getMessage(), module);
                    try {
                        TransactionUtil.rollback(beganTransaction, "Error handling product catalog", e);
                    } catch (GenericTransactionException rollbackException) {
                        Debug.logError("Transaction rollback failed: " + rollbackException.getMessage(), module);
                    }
                    return ServiceUtil.returnError("Error handling product catalog: " + e.getMessage());
                }

                try {
                    Debug.logInfo("Checking if Product Category exists with ID: " + productCategoryId, module);
                    GenericValue existingProductCategory = EntityQuery.use(delegator)
                            .from("ProductCategory")
                            .where("productCategoryId", productCategoryId)
                            .queryOne();

                    if (existingProductCategory == null) {
                        Debug.logInfo("Creating new Product Category with ID: " + productCategoryId, module);
                        dispatcher.runSync("createProductCategory", UtilMisc.toMap(
                                "productCategoryId", productCategoryId,
                                "productCategoryTypeId", productCategoryTypeId,
                                "primaryParentCategoryId", primaryParentCategoryId,
                                "categoryName", categoryName,
                                "description", description,
                                "userLogin", permUserLogin
                        ));
                    } else {
                        Debug.logInfo("Updating existing Product Category with ID: " + productCategoryId, module);
                        existingProductCategory.setString("categoryName", categoryName);
                        existingProductCategory.setString("description", description);
                        existingProductCategory.store();
                    }
                } catch (GenericServiceException e) {
                    Debug.logError("Error handling product category: " + e.getMessage(), module);
                    try {
                        TransactionUtil.rollback(beganTransaction, "Error handling product category", e);
                    } catch (GenericTransactionException rollbackException) {
                        Debug.logError("Transaction rollback failed: " + rollbackException.getMessage(), module);
                    }
                    return ServiceUtil.returnError("Error handling product category: " + e.getMessage());
                }

                try {
                    Debug.logInfo("Creating Product Catalog Category with ID: " + prodCatalogCategoryTypeId, module);
                    dispatcher.runSync("addProductCategoryToProdCatalog", UtilMisc.toMap(
                            "prodCatalogId", prodCatalogId,
                            "productCategoryId", productCategoryId,
                            "prodCatalogCategoryTypeId", prodCatalogCategoryTypeId,
                            "fromDate", fromDate,
                            "userLogin", permUserLogin
                    ));
                } catch (GenericServiceException e) {
                    Debug.logError("Unable to create product catalog category: " + e.getMessage(), module);
                    try {
                        TransactionUtil.rollback(beganTransaction, "Error creating product catalog category", e);
                    } catch (GenericTransactionException rollbackException) {
                        Debug.logError("Transaction rollback failed: " + rollbackException.getMessage(), module);
                    }
                    return ServiceUtil.returnError("Error creating product catalog category: " + e.getMessage());
                }
                try {
                    Debug.logInfo("Checking if Product Feature exists with ID: " + productFeatureId, module);
                    GenericValue existingProductFeature = EntityQuery.use(delegator)
                            .from("ProductFeature")
                            .where("productFeatureId", productFeatureId)
                            .queryOne();

                    if (existingProductFeature == null) {
                        Debug.logInfo("Creating new Product Feature with ID: " + productFeatureId, module);
                        dispatcher.runSync("createProductFeature", UtilMisc.toMap(
                                "productFeatureId", productFeatureId,
                                "productFeatureTypeId", productFeatureTypeId,
                                "description", description,
                                "userLogin", permUserLogin
                        ));
                    } else {
                        Debug.logInfo("Updating existing Product Feature with ID: " + productFeatureId, module);
                        existingProductFeature.setString("description", description);
                        existingProductFeature.store();
                    }
                } catch (GenericServiceException e) {
                    Debug.logError("Error handling product feature: " + e.getMessage(), module);
                    try {
                        TransactionUtil.rollback(beganTransaction, "Error handling product feature", e);
                    } catch (GenericTransactionException rollbackException) {
                        Debug.logError("Transaction rollback failed: " + rollbackException.getMessage(), module);
                    }
                    return ServiceUtil.returnError("Error handling product feature: " + e.getMessage());
                }

                try {
                    Debug.logInfo("Creating Product Price with ID: " + productPriceTypeId, module);
                    dispatcher.runSync("createProductPrice", UtilMisc.toMap(
                            "productId", productId,
                            "productPriceTypeId", productPriceTypeId,
                            "productPricePurposeId", productPricePurposeId,
                            "currencyUomId", currencyUomId,
                            "price", price,
                            "productStoreGroupId", productStoreGroupId,
                            "fromDate", fromDate,
                            "userLogin", permUserLogin
                    ));
                } catch (GenericServiceException e) {
                    Debug.logError("Unable to create product price: " + e.getMessage(), module);
                    try {
                        TransactionUtil.rollback(beganTransaction, "Error creating product price", e);
                    } catch (GenericTransactionException rollbackException) {
                        Debug.logError("Transaction rollback failed: " + rollbackException.getMessage(), module);
                    }
                    return ServiceUtil.returnError("Error creating product price: " + e.getMessage());
                }
                try {
                    Debug.logInfo("Creating Product Feature Appl with ID: " + productFeatureApplTypeId, module);
                    dispatcher.runSync("applyFeatureToProduct", UtilMisc.toMap(
                            "productId", productId,
                            "productFeatureId", productFeatureId,
                            "productFeatureApplTypeId", productFeatureApplTypeId,
                            "fromDate", fromDate,
                            "userLogin", permUserLogin
                    ));
                } catch (GenericServiceException e) {
                    Debug.logError("Unable to create product feature appl: " + e.getMessage(), module);
                    try {
                        TransactionUtil.rollback(beganTransaction, "Error creating product feature appl", e);
                    } catch (GenericTransactionException rollbackException) {
                        Debug.logError("Transaction rollback failed: " + rollbackException.getMessage(), module);
                    }
                    return ServiceUtil.returnError("Error creating product feature appl: " + e.getMessage());
                }
                try {
                    Debug.logInfo("Creating Product Store Catalog with ID: " + productStoreId, module);
                    dispatcher.runSync("createProductStoreCatalog", UtilMisc.toMap(
                            "productStoreId", productStoreId,
                            "prodCatalogId", prodCatalogId,
                            "fromDate", fromDate,
                            "userLogin", permUserLogin
                    ));
                } catch (GenericServiceException e) {
                    Debug.logError("Unable to create product store catalog: " + e.getMessage(), module);
                    try {
                        TransactionUtil.rollback(beganTransaction, "Error creating product store catalog", e);
                    } catch (GenericTransactionException rollbackException) {
                        Debug.logError("Transaction rollback failed: " + rollbackException.getMessage(), module);
                    }
                    return ServiceUtil.returnError("Error creating product store catalog: " + e.getMessage());
                }
                try {
                    Debug.logInfo("Creating Product Category Member with ID: " + productCategoryId, module);
                    dispatcher.runSync("addProductToCategory", UtilMisc.toMap(
                            "productCategoryId", productCategoryId,
                            "productId", productId,
                            "fromDate", fromDate,
                            "userLogin", permUserLogin
                    ));
                } catch (GenericServiceException e) {
                    Debug.logError("Unable to create product category member: " + e.getMessage(), module);
                    try {
                        TransactionUtil.rollback(beganTransaction, "Error creating product category member", e);
                    } catch (GenericTransactionException rollbackException) {
                        Debug.logError("Transaction rollback failed: " + rollbackException.getMessage(), module);
                    }
                    return ServiceUtil.returnError("Error creating product category member: " + e.getMessage());
                }
            }
            reader.close();
            TransactionUtil.commit(beganTransaction);
            Debug.logInfo("CSV import completed successfully", module);
            result.put("message", "Files imported successfully");
            Debug.logInfo("Files imported successfully", module);
        } catch (Exception e) {
            Debug.logError("Error importing CSV: " + e.getMessage(), module);
            try {
                TransactionUtil.rollback(beganTransaction, "Error during CSV import", e);
            } catch (GenericTransactionException rollbackException) {
                Debug.logError("Transaction rollback failed: " + rollbackException.getMessage(), module);
            }
            return ServiceUtil.returnError("Error importing CSV: " + e.getMessage());
        }

        return result;
    }

    private static Map<String, Integer> getColumnIndexMap(String[] headers) {
        Map<String, Integer> headerIndexMap = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            headerIndexMap.put(headers[i].trim().toLowerCase(), i);
        }
        return headerIndexMap;
    }

    private static String getValue(String[] fields, Map<String, Integer> headerIndexMap, String columnName) {
        Integer index = headerIndexMap.get(columnName.toLowerCase());
        return (index != null && index < fields.length) ? fields[index].trim() : "";
    }
}
