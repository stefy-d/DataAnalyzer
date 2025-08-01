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
+ pornesc cate un proces pentru fiecare consumer si un proces care sa imi creeze producerii in threaduri

### *`producers`*
+ pornesc cate un thread pentru fiecare fisier in care deschid cate un producer care sa faca streaming, fiecare pe un topic specific
+ impart fisierul in batch-uri, cate 50 de linii

### *`consumers`*
+ am cate un proces pt fiecare consumer ca sa pot sa deschid cate o sesiune spark care sa mi analizeze datele
+ am facut sa pun datele primite in df 

### *`schema`*
+ am construit pentru fiecare fisier schema ca sa pot pune datele in df si sa le analizez in consumer pe masura ce primesc batchurile


<br>

      Nu am reusit sa folosesc spark streaming pt ca nu am putut sa leg sesiunea spark de docker ul in care am consumerii si producerii asa ca fac cate o sesiune in fiecare             consumer ca sa poata accesa datele.
      Problema urmatoaare va fi cum voi retine rezultatele in acelasi loc si cum pot sa fac o statistica overall or smth. <3
