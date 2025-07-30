# DataAnalyzer

### *`run_all`*
+ porneste preprocesarea pentru toate fisierele

### *`prccess_data`*
Curata datele:
+ elimin coloanele si apoi randurile care au majoritatea valorilor lipsa
+ fac cast la float pentru coloanele care ar trebui sa fie numerice
+ elimin trailing whitespaces din continutul inregistrarilor
+ folosesc mediana pentru a completa valorile nule

### *`run_prod_cons`*
+ pornesc cate un proces pentru streamingul de date: producatori, consmatori

### *`producers`*
+ pornesc cate un thread pentru fiecare fisier in care deschid cate un producer care sa faca streaming, fiecare pe un topic specific
+ impart fisierul in batch-uri, cate 50 de linii

### *`consumers`*
+ pornesc cate un thread in care deschid cate un consumer care sa faca poll fiecare pe cate un topic
+ consumerii primesc date si mai departe trebuie sa inceapa analiza in Spark: DBD
+ cand un consumer nu mai primeste nimic 10s, se inchide consumerul
