# semantix-exam

<b>1- Qual o objetivo do comando cache em Spark?</b>

O objetivo do comando cache é de armazenar em memoria os registros para facilitar compartilhamento e acesso aos dados evitando que os mesmos registros sejam sempre carregados ao executar a mesma ação. 


<b>2- O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?</b>

Uma das características que afetam a performance no MapReduce é a não utilização da memória para armazenamento de dados em processos que são iterativos e compartilhados entre os nós, isto contribui para o aumento de I/O e reduz consideravelmente o tempo de processamento. No Spark pode-se trabalhar com os dados em memória quando se é executado uma ação facilitando o acesso para os processos iterativos.


<b>3- Qual é a função do SparkContext?</b>

Basicamente a função é criar um ponteiro para o datasource que será utilizado pelo RDD, assim é possível configurar se será executado localmente ou em será compartilhado em um cluster, ou a quantidade de memória a ser utilizada e processamento.


<b>4- Explique com suas palavras o que é Resilient Distributed Datasets (RDD).</b>

O RDD (Resilient Distributed Dataset) é uma amostra/padrão definido através de ações e processos de transformações que podem ser distribuídos e/ou compartilhados entre os nós em cluster, quando compartilhado é possível processar o mesmo RDD em paralelo nos nós. Possui uma característica em que não se pode alterar o seu dados ou estrutura para isto é necessário a criação de uma nova amostragem.   

<b>5- GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?</b>

O GroupByKey e reduceByKey podem produzir o mesmo resultado, porém a maneira como é feito o processamento é distinto afetando assim a performance quando se é utilizado o GroupByKey. Isto ocorre devido como os pares de chave e valor são consolidados no final e distribuídos nas partições, ocasionando consumo de recursos desnecessários para transferência dos dados.
Diferentemente, o reduceByKey executa a consolidação através da classificação de um padrão em cada partição antes de disponibilizar os dados.


<b>Explique o que o código Scala abaixo faz.</b>

<i>-- Leitura do arquivo texto</i>

val textFile = sc.textFile("hdfs://...")

<i>-- Define a delimitação de cada palavra de acordo com espaço, criando assim um padrão(RDD).</i>

val counts = textFile.flatMap(line => line.split(" "))

<i>-- Todos as palavras são mapeadas em uma lista de chave e valor cuja chave é representada pela palavra e o valor é um constante armazenado com o valor numérico 1.</i>

.map(word => (word, 1))

<i>-- Somatória dos valores da lista de acordo com a chave (cada palavra).</i>

.reduceByKey(_ + _)

<i>-- Criação de arquivo texto com a lista da quantidade de palavras.</i>

counts.saveAsTextFile("hdfs://...")
