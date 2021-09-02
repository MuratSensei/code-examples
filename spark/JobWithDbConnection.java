package spark;

public class JobWithDbConnection {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
            .appName("spark.job.with.db.connection")
            .master("local")
            .getOrCreate();

        sparkSession.read()
            .format("jdbc")
            .option("url", "jdbc:sqlserver://localhost;databaseName=TestDb")
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("dbtable", "dbo.Test")
            .option("user", "kara")
            .option("password", "123456")
            .load()
            .createOrReplaceTempView("my_test_table");

        final String query = "SELECT id FROM my_test_table WHERE type_id = 3";

        List<Row> queryResult = sparkSession.sql(query).collectAsList();

        RabbitTemplate rabbitTemplate = new RabbitTemplate();

        queryResult.forEach(row -> {
            int id = row.getAs("id");
            rabbitTemplate.convertAndSend("rabbitmq.test.exchange", "", new SparkExampleModel(id));
        });

        rabbitTemplate.destroy();
    }
}
