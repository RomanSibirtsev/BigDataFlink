package ru.bigdata.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SaleEvent implements Serializable {
    public String id;
    public String customer_first_name;
    public String customer_last_name;
    public Integer customer_age;
    public String customer_email;
    public String customer_country;
    public String customer_postal_code;
    public String customer_pet_type;
    public String customer_pet_name;
    public String customer_pet_breed;
    public String seller_first_name;
    public String seller_last_name;
    public String seller_email;
    public String seller_country;
    public String seller_postal_code;
    public String product_name;
    public String product_category;
    public BigDecimal product_price;
    public Integer product_quantity;
    public String sale_date;
    public String sale_customer_id;
    public String sale_seller_id;
    public String sale_product_id;
    public Integer sale_quantity;
    public BigDecimal sale_total_price;
    public String store_name;
    public String store_location;
    public String store_city;
    public String store_state;
    public String store_country;
    public String store_phone;
    public String store_email;
    public String pet_category;
    public BigDecimal product_weight;
    public String product_color;
    public String product_size;
    public String product_brand;
    public String product_material;
    public String product_description;
    public BigDecimal product_rating;
    public Integer product_reviews;
    public String product_release_date;
    public String product_expiry_date;
    public String supplier_name;
    public String supplier_contact;
    public String supplier_email;
    public String supplier_phone;
    public String supplier_address;
    public String supplier_city;
    public String supplier_country;
    public String source_file;
    public Long source_row;

    public static SaleEvent from(Map<String, String> row, String sourceFile, long sourceRow) {
        SaleEvent event = new SaleEvent();
        event.id = row.get("id");
        event.customer_first_name = row.get("customer_first_name");
        event.customer_last_name = row.get("customer_last_name");
        event.customer_age = integer(row.get("customer_age"));
        event.customer_email = row.get("customer_email");
        event.customer_country = row.get("customer_country");
        event.customer_postal_code = row.get("customer_postal_code");
        event.customer_pet_type = row.get("customer_pet_type");
        event.customer_pet_name = row.get("customer_pet_name");
        event.customer_pet_breed = row.get("customer_pet_breed");
        event.seller_first_name = row.get("seller_first_name");
        event.seller_last_name = row.get("seller_last_name");
        event.seller_email = row.get("seller_email");
        event.seller_country = row.get("seller_country");
        event.seller_postal_code = row.get("seller_postal_code");
        event.product_name = row.get("product_name");
        event.product_category = row.get("product_category");
        event.product_price = decimal(row.get("product_price"));
        event.product_quantity = integer(row.get("product_quantity"));
        event.sale_date = row.get("sale_date");
        event.sale_customer_id = row.get("sale_customer_id");
        event.sale_seller_id = row.get("sale_seller_id");
        event.sale_product_id = row.get("sale_product_id");
        event.sale_quantity = integer(row.get("sale_quantity"));
        event.sale_total_price = decimal(row.get("sale_total_price"));
        event.store_name = row.get("store_name");
        event.store_location = row.get("store_location");
        event.store_city = row.get("store_city");
        event.store_state = row.get("store_state");
        event.store_country = row.get("store_country");
        event.store_phone = row.get("store_phone");
        event.store_email = row.get("store_email");
        event.pet_category = row.get("pet_category");
        event.product_weight = decimal(row.get("product_weight"));
        event.product_color = row.get("product_color");
        event.product_size = row.get("product_size");
        event.product_brand = row.get("product_brand");
        event.product_material = row.get("product_material");
        event.product_description = row.get("product_description");
        event.product_rating = decimal(row.get("product_rating"));
        event.product_reviews = integer(row.get("product_reviews"));
        event.product_release_date = row.get("product_release_date");
        event.product_expiry_date = row.get("product_expiry_date");
        event.supplier_name = row.get("supplier_name");
        event.supplier_contact = row.get("supplier_contact");
        event.supplier_email = row.get("supplier_email");
        event.supplier_phone = row.get("supplier_phone");
        event.supplier_address = row.get("supplier_address");
        event.supplier_city = row.get("supplier_city");
        event.supplier_country = row.get("supplier_country");
        event.source_file = sourceFile;
        event.source_row = sourceRow;
        return event;
    }

    private static Integer integer(String value) {
        return value == null || value.isBlank() ? null : Integer.valueOf(value);
    }

    private static BigDecimal decimal(String value) {
        return value == null || value.isBlank() ? null : new BigDecimal(value);
    }
}