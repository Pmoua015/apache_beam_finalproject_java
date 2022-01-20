import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

import com.google.api.services.bigquery.model.TableFieldSchema;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        pipelineOptions.setJobName("pm-java-prac");
        pipelineOptions.setProject("york-cdf-start");
        pipelineOptions.setRegion("us-central1");
        pipelineOptions.setRunner(DataflowRunner.class);

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        TableSchema products_schema = new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("cust_tier_code").setType("STRING").setMode("REQUIRED"),
                new TableFieldSchema().setName("sku").setType("INTEGER").setMode("REQUIRED"),
                new TableFieldSchema().setName("total_no_of_product_views").setType("INTEGER").setMode("REQUIRED")
        ));
        TableSchema total_sales_schema = new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("cust_tier_code").setType("STRING").setMode("REQUIRED"),
                new TableFieldSchema().setName("sku").setType("INTEGER").setMode("REQUIRED"),
                new TableFieldSchema().setName("total_sales_amount").setType("FLOAT").setMode("REQUIRED")
        ));

        PCollection<TableRow> products_view_rows =
                pipeline.apply(
                        "Read from BigQuery query",
                        BigQueryIO.readTableRows()
                                .fromQuery("SELECT CAST(c.CUST_TIER_CODE as STRING) as CUST_TIER_CODE ,pv.SKU,COUNT(c.CUSTOMER_ID) as total_no_of_product_views FROM `york-cdf-start.final_input_data.product_views` as pv \n" +
                                        "JOIN `york-cdf-start.final_input_data.customers` as c \n" +
                                        "ON pv.CUSTOMER_ID=c.CUSTOMER_ID GROUP BY c.CUST_TIER_CODE,pv.SKU")
                                .usingStandardSql());
        PCollection<TableRow> total_sales_rows =
                pipeline.apply(
                        "Read from BigQuery  orders table",
                        BigQueryIO.readTableRows()
                                .fromQuery("SELECT CAST(c.CUST_TIER_CODE as STRING) as CUST_TIER_CODE,ts.SKU, SUM(ts.ORDER_AMT) as total_sales_amount  FROM `york-cdf-start.final_input_data.orders` as ts \n" +
                                        "join `york-cdf-start.final_input_data.customers` as c \n" +
                                        "on ts.CUSTOMER_ID=c.CUSTOMER_ID group by ts.SKU,c.CUST_TIER_CODE")
                                .usingStandardSql());

        products_view_rows.apply(
                "Write to BigQuery1",
                BigQueryIO.writeTableRows()
                        .to("york-cdf-start:final_paul_moua.cust_tier_code-sku-total_no_of_product_views_javaprac2")
                        .withSchema(products_schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        total_sales_rows.apply(
                "Write to BigQuery2",
                BigQueryIO.writeTableRows()
                        .to("york-cdf-start:final_paul_moua.cust_tier_code-sku-total_sales_amount_javaprac2")
                        .withSchema(total_sales_schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        pipeline.run().waitUntilFinish();

    }
}
