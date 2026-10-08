package ru.bigdata.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class PostgresStarSink extends RichSinkFunction<StarRecord> {
    private final String url;
    private final String user;
    private final String password;

    private transient Connection connection;
    private transient PreparedStatement customer;
    private transient PreparedStatement seller;
    private transient PreparedStatement supplier;
    private transient PreparedStatement product;
    private transient PreparedStatement store;
    private transient PreparedStatement date;
    private transient PreparedStatement sale;

    public PostgresStarSink(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(url, user, password);
        connection.setAutoCommit(false);
        customer = connection.prepareStatement(
                "INSERT INTO dim_customer VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT (customer_key) DO NOTHING");
        seller = connection.prepareStatement(
                "INSERT INTO dim_seller VALUES (?,?,?,?,?,?) ON CONFLICT (seller_key) DO NOTHING");
        supplier = connection.prepareStatement(
                "INSERT INTO dim_supplier VALUES (?,?,?,?,?,?,?,?) ON CONFLICT (supplier_key) DO NOTHING");
        product = connection.prepareStatement(
                "INSERT INTO dim_product VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (product_key) DO NOTHING");
        store = connection.prepareStatement(
                "INSERT INTO dim_store VALUES (?,?,?,?,?,?,?,?) ON CONFLICT (store_key) DO NOTHING");
        date = connection.prepareStatement(
                "INSERT INTO dim_date (date_key, day, month, quarter, year) "
                        + "VALUES (?, EXTRACT(DAY FROM CAST(? AS DATE)), "
                        + "EXTRACT(MONTH FROM CAST(? AS DATE)), "
                        + "EXTRACT(QUARTER FROM CAST(? AS DATE)), "
                        + "EXTRACT(YEAR FROM CAST(? AS DATE))) ON CONFLICT (date_key) DO NOTHING");
        sale = connection.prepareStatement(
                "INSERT INTO fact_sales VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (sale_key) DO NOTHING");
    }

    @Override
    public void invoke(StarRecord record, Context context) throws Exception {
        SaleEvent event = record.event;
        try {
            set(customer, record.customerKey, event.customer_first_name, event.customer_last_name,
                    event.customer_age, event.customer_email, event.customer_country,
                    event.customer_postal_code, event.customer_pet_type, event.customer_pet_name,
                    event.customer_pet_breed);
            customer.executeUpdate();

            set(seller, record.sellerKey, event.seller_first_name, event.seller_last_name,
                    event.seller_email, event.seller_country, event.seller_postal_code);
            seller.executeUpdate();

            set(supplier, record.supplierKey, event.supplier_name, event.supplier_contact,
                    event.supplier_email, event.supplier_phone, event.supplier_address,
                    event.supplier_city, event.supplier_country);
            supplier.executeUpdate();

            set(product, record.productKey, event.product_name, event.product_category,
                    event.product_price, event.product_quantity, event.pet_category,
                    event.product_weight, event.product_color, event.product_size,
                    event.product_brand, event.product_material, event.product_description,
                    event.product_rating, event.product_reviews, record.releaseDate, record.expiryDate);
            product.executeUpdate();

            set(store, record.storeKey, event.store_name, event.store_location, event.store_city,
                    event.store_state, event.store_country, event.store_phone, event.store_email);
            store.executeUpdate();

            set(date, record.saleDate, record.saleDate, record.saleDate, record.saleDate, record.saleDate);
            date.executeUpdate();

            set(sale, record.saleKey, record.customerKey, record.sellerKey, record.productKey,
                    record.storeKey, record.supplierKey, record.saleDate, event.sale_quantity,
                    event.sale_total_price, event.source_file, event.source_row);
            sale.executeUpdate();
            connection.commit();
        } catch (Exception exception) {
            connection.rollback();
            throw exception;
        }
    }

    private static void set(PreparedStatement statement, Object... values) throws Exception {
        statement.clearParameters();
        for (int index = 0; index < values.length; index++) {
            statement.setObject(index + 1, values[index]);
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
}