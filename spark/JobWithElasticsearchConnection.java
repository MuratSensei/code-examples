package spark;

public class JobWithElasticsearchConnection {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
            .appName("spark.job.with.elasticsearch.connection")
            .master("local")
            .config("es.nodes", "localhost")
            .config("es.port", "9200")
            .config("es.index.read.missing.as.empty", "true")
            .config("spark.driver.allowMultipleContext", "true")
            .getOrCreate();

        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"term\": {\n" +
                "            \"surname\": \"kara\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"size\": 0\n" +
                "}";

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        JavaPairRDD<String, Map<String, Object>> result = JavaEsSpark.esRDD(jsc, "spark_example", query);

        result.repartition(1).foreachPartition(object -> {
            RabbitTemplate rabbitTemplate = new RabbitTemplate();

            object.forEachRemaining(i -> {
                Map<String, Object> fieldMap = i._2;
                SparkExampleModel sparkExampleModel = new SparkExampleModel();
                sparkExampleModel.setId(fieldMap.get("id").toString());
                
                rabbitTemplate.convertAndSend("rabbitmq.test.exchange", "", sparkExampleModel);
            });

            rabbitTemplate.destroy();
        });
    }
}
