
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Teste {

	public static void main(String[] args) {

		// configuração do Spark
		SparkConf conf = new SparkConf().setMaster("local").setAppName("BusProcessor");
		JavaSparkContext ctx = new JavaSparkContext(conf);

		// carrega os dados dos ônibus de sp
		JavaRDD<String> linhas = ctx.textFile("C:/Users/villa/Downloads/Desafio/access_log_Jul95");
		long numeroLinhas = linhas.count();

		// escreve o número de ônibus que existem no arquivo
		System.out.println(numeroLinhas);

		ctx.close();
	}

}
