package ru.bigdata.flink;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

public class StarRecord implements Serializable {
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("M/d/yyyy");

    public SaleEvent event;
    public UUID customerKey;
    public UUID sellerKey;
    public UUID productKey;
    public UUID storeKey;
    public UUID supplierKey;
    public UUID saleKey;
    public Date saleDate;
    public Date releaseDate;
    public Date expiryDate;

    public static StarRecord from(SaleEvent event) {
        StarRecord record = new StarRecord();
        record.event = event;
        record.customerKey = key("customer", event.customer_first_name, event.customer_last_name,
                String.valueOf(event.customer_age), event.customer_email, event.customer_country,
                event.customer_postal_code, event.customer_pet_type, event.customer_pet_name,
                event.customer_pet_breed);
        record.sellerKey = key("seller", event.seller_first_name, event.seller_last_name,
                event.seller_email, event.seller_country, event.seller_postal_code);
        record.productKey = key("product", event.sale_product_id, event.product_name,
                event.product_category, string(event.product_price), string(event.product_quantity),
                event.pet_category, string(event.product_weight), event.product_color,
                event.product_size, event.product_brand, event.product_material,
                event.product_description, string(event.product_rating),
                string(event.product_reviews), event.product_release_date, event.product_expiry_date);
        record.storeKey = key("store", event.store_name, event.store_location, event.store_city,
                event.store_state, event.store_country, event.store_phone, event.store_email);
        record.supplierKey = key("supplier", event.supplier_name, event.supplier_contact,
                event.supplier_email, event.supplier_phone, event.supplier_address,
                event.supplier_city, event.supplier_country);
        record.saleKey = key("sale", event.source_file, String.valueOf(event.source_row));
        record.saleDate = date(event.sale_date);
        record.releaseDate = date(event.product_release_date);
        record.expiryDate = date(event.product_expiry_date);
        return record;
    }

    private static String string(Object value) {
        return value == null ? null : value.toString();
    }

    private static UUID key(String... parts) {
        String value = Arrays.stream(parts)
                .map(part -> part == null ? "" : part)
                .map(part -> part.length() + ":" + part)
                .collect(Collectors.joining("|"));
        return UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8));
    }

    private static Date date(String value) {
        return value == null || value.isBlank()
                ? null
                : Date.valueOf(LocalDate.parse(value, DATE_FORMAT));
    }
}