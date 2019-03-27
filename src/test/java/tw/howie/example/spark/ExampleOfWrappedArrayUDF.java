package tw.howie.example.spark;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.createStructField;

/**
 * @author howie
 * @since 2019-03-27
 * <p>
 * For
 * https://stackoverflow.com/questions/55357655/how-to-cast-datasetrow-columns-to-non-primitive-data-type
 */
public class ExampleOfWrappedArrayUDF {

    String master = "local[2]";
    private SparkSession spark = SparkSession.builder()
                                             .appName("ExampleOfWrappedArrayUDF")
                                             .master(master)
                                             .config("spark.submit.deployMode", "client")
                                             .config("spark.io.compression.codec", "snappy")
                                             .config("spark.rdd.compress", "true")
                                             .getOrCreate();

    @Before
    public void before() {
        PropertyConfigurator.configure(ExampleOfWrappedArrayUDF.class.getClassLoader()
                                                                     .getResource("log4j-test.properties"));
    }

    @After
    public void after() {
        spark.stop();
    }

    @Test
    public void testExampleOfWrappsArrayUDF() {

        /*
         +------+---------------+---------------------------------------------+---------------+
         |    Id| value         |     time                                      |aggregateType  |
         +------+---------------+---------------------------------------------+---------------+
         |0001  |  [1.5,3.4,4.5]| [1551502200000,1551502200000,1551502200000] | Sum             |
         +------+---------------+---------------------------------------------+---------------+
         **/

        StructType dataSchema = new StructType(new StructField[] {createStructField("Id", DataTypes.StringType, true),
                                                                  createStructField("value",
                                                                                    DataTypes.createArrayType(DataTypes.DoubleType,
                                                                                                              false),
                                                                                    false),

                                                                  createStructField("time",
                                                                                    DataTypes.createArrayType(DataTypes.LongType,
                                                                                                              false),
                                                                                    false),
                                                                  createStructField("aggregateType",
                                                                                    DataTypes.StringType,
                                                                                    true),});

        List<Row> data = new ArrayList<>();

        data.add(RowFactory.create("0001",
                                   Arrays.asList(1.5, 3.4, 4.5),
                                   Arrays.asList(1551502200000L, 1551502200000L, 1551502200000L),
                                   "sum"));
        Dataset<Row> example = spark.createDataFrame(data, dataSchema);
        example.show(false);

        UDF3<String, WrappedArray<Long>, WrappedArray<Double>, Double> myUDF = (param1, param2, param3) -> {

            List<Long> param1AsList = JavaConversions.seqAsJavaList(param2);
            List<Double> param2AsList = JavaConversions.seqAsJavaList(param3);

            //Example
            double myDoubleResult = 0;
            if ("sum".equals(param1)) {

                myDoubleResult = param2AsList.stream()
                                             .mapToDouble(f -> f)
                                             .sum();
            }

            return myDoubleResult;
        };

        spark.udf()
             .register("myUDF", myUDF, DataTypes.DoubleType);

        example = example.withColumn("new", callUDF("myUDF", col("aggregateType"), col("time"), col("value")));
        example.show(false);

    }

}
