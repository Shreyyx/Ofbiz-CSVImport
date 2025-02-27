package com.companyname.ofbizdemo.services;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.ofbiz.base.util.Debug;
import org.apache.ofbiz.service.DispatchContext;
import java.util.Map;
import java.io.File;

public class ImportCsv {
    public static final String module = ImportCsv.class.getName();

    public static void importCsv(DispatchContext dctx, Map<String, ?> context) {
        String csvFilePath = (String) context.get("csvFilePath");

        String delimiter = ",";
        Debug.logInfo("-------- " + csvFilePath + "----------", module);
        File csvFile = new File(csvFilePath);

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(delimiter);
                if (fields.length > 17) {
                    String productId = fields[0].trim();
                    String productTypeId = fields[1].trim();
                    String internalName = fields[2].trim();
                    String prodCatalogId = fields[3].trim();
                    String catalogName = fields[4].trim();
                    String productStoreId = fields[5].trim();
                    String productCategoryId = fields[6].trim();
                    String productCategoryTypeId = fields[7].trim();
                    String primaryParentCategoryId = fields[8].trim();
                    String prodCatalogCategoryTypeId = fields[9].trim();
                    String productPriceTypeId = fields[10].trim();
                    String productPricePurposeId = fields[11].trim();
                    String storeName = fields[12].trim();
                    String priceStr = fields[13].trim();
                    String productFeatureTypeId = fields[14].trim();
                    String productFeatureId = fields[15].trim();
                    String productFeatureCategoryId = fields[16].trim();
                    String productFeatureApplTypeId = fields[17].trim();
                    String fromDateStr = fields[18].trim();

                    Debug.logInfo("-----" + prodCatalogCategoryTypeId + " " + productId + "------", module);
                } else {
                    Debug.logWarning("Skipping line due to insufficient fields: " + line, module);
                }
            }
        } catch (IOException e) {
            Debug.logError(e, "Error reading CSV file", module);
        }
    }
}
