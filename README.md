<h2>Chargement des données dans une base Hive</h2>

Récupération des fichiers CSV et suppression des premières lignes car elles contiennnent les titres des attributs

~~~~~~~~~~~~~~
wget https://blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/72fc17/hive-data-warehouse-dataset.zip
unzip hive-data-warehouse-dataset.zip
cd hive-dataset
sed -i '1d' *.csv
~~~~~~~~~~~~~~

Création du dépot de fichiers CSV dans HDFS
~~~~~~~~~~~~~~
hadoop fs -mkdir /table
hadoop fs -copyFromLocal aisles.csv /table/aisles.csv
hadoop fs -copyFromLocal departments.csv /table/departments.csv
hadoop fs -copyFromLocal products.csv /table/products.csv
hadoop fs -copyFromLocal orders.csv /table/orders.csv
hadoop fs -copyFromLocal order_products.csv /table/order_products.csv
~~~~~~~~~~~~~~

Accéder à Hive
~~~~~~~~~~~~~~
hive
~~~~~~~~~~~~~~

Création des tables sans référence externes aux fichiers CSV.

~~~~~~~~~~~~~~
CREATE DATABASE essou;

create external table if not exists essou.aisles (aisle_id Int,aisle String, PRIMARY KEY (aisle_id) DISABLE NOVALIDATE)
comment 'Aisle Table'
row format delimited
fields terminated by ',';

create external table if not exists essou.departments (department_id Int,department String, PRIMARY KEY (department_id) DISABLE NOVALIDATE)
comment 'department Table'
row format delimited
fields terminated by ',';

create external table if not exists essou.products (product_id Int, product_name String, aisle_id Int, department_id Int, PRIMARY KEY (product_id) DISABLE NOVALIDATE, CONSTRAINT fk FOREIGN KEY (aisle_id) REFERENCES essou.aisles(aisle_id) DISABLE NOVALIDATE, CONSTRAINT fkb FOREIGN KEY (department_id) REFERENCES essou.departments(department_id) DISABLE NOVALIDATE)
comment 'products Table'
row format delimited
fields terminated by ',';

create external table if not exists essou.orders (order_id INT ,user_id INT, eval_set String, order_number INT, order_dow INT, order_hour_of_day INT, days_since_prior_order DOUBLE, PRIMARY KEY (order_id) DISABLE NOVALIDATE)
comment 'orders Table'
row format delimited
fields terminated by ',';

create external table if not exists essou.order_products (order_id INT ,product_id INT, add_to_cart_order INT, reordered INT, CONSTRAINT fka FOREIGN KEY (order_id) REFERENCES essou.orders(order_id) DISABLE NOVALIDATE, CONSTRAINT fkc FOREIGN KEY (product_id) REFERENCES essou.products(product_id) DISABLE NOVALIDATE)
comment 'order_products Table'
row format delimited
fields terminated by ',';
~~~~~~~~~~~~~~

Insertion des données CSV dans les différentes tables
~~~~~~~~~~~~~~
LOAD DATA INPATH '/table/aisles.csv' INTO TABLE essou.aisles;
LOAD DATA INPATH '/table/departments.csv' INTO TABLE essou.departments;
LOAD DATA INPATH '/table/products.csv' INTO TABLE essou.products;
LOAD DATA INPATH '/table/orders.csv' INTO TABLE essou.orders;
LOAD DATA INPATH '/table/order_products.csv' INTO TABLE essou.order_products;
~~~~~~~~~~~~~~

Création des tables orders_p & order_products_p qui sont partitionnées par rapport à leurs prédecésseurs (orders et order_products).

~~~~~~~~~~~~~~
CREATE EXTERNAL TABLE IF NOT EXISTS essou.orders_p (order_id INT, user_id INT, order_number INT, days_since_prior_order DOUBLE, eval_set String, order_hour_of_day INT) 
PARTITIONED BY (order_dow INT);
insert overwrite table essou.orders_p select order_id ,user_id, order_number, days_since_prior_order, eval_set, order_hour_of_day, order_dow from essou.orders;
~~~~~~~~~~~~~~

~~~~~~~~~~~~~~
CREATE EXTERNAL TABLE IF NOT EXISTS essou.order_products_p (order_id INT ,product_id INT, add_to_cart_order INT) 
PARTITIONED BY (reordered INT);
insert overwrite table essou.order_products_p select order_id, product_id, add_to_cart_order, reordered from essou.order_products;
~~~~~~~~~~~~~~

<h2>Tables analytiques</h2>

Une table aisles_products dénombrant le nombre d'articles dans chaque catégorie

~~~~~~~~~~~~~~
create table if not exists essou.aisles_products (id_aisle INT, aisle STRING, number INT);
insert overwrite table essou.aisles_products select T1.aisle_id, T1.aisle, T2.county from essou.aisles as T1 LEFT JOIN (select aisle_id, count(*) as county from essou.products GROUP BY aisle_id) as T2 ON (T1.aisle_id = T2.aisle_id) GROUP BY T1.aisle_id, T1.aisle, T2.county;
~~~~~~~~~~~~~~

Une table departments_products dénombrant le nombre d'articles dans chaque département.

~~~~~~~~~~~~~~
create table if not exists essou.department_products (department_id INT, department STRING, number INT);
insert overwrite table essou.department_products select T1.department_id, T1.department, T2.county from essou.departments as T1 LEFT JOIN (select department_id, count(*) as county from essou.products GROUP BY department_id) as T2 ON (T1.department_id = T2.department_id) GROUP BY T1.department_id, T1.department, T2.county;
~~~~~~~~~~~~~~

Une table orders_per_users dénombrant le nombre d'articles moyen par commande, et ce pour chaque utilisateur.

~~~~~~~~~~~~~~
create table if not exists essou.orders_per_users (user_id INT, count_products_per_cmd INT);
insert overwrite table essou.orders_per_users select T1.user_id, AVG(T2.maxi) as am from essou.orders as T1 LEFT JOIN (select order_id, MAX(add_to_cart_order) as maxi FROM essou.order_products_p GROUP BY order_id) AS T2 ON (T1.order_id = T2.order_id) GROUP BY T1.user_id HAVING am > 0;
~~~~~~~~~~~~~~

<h2>Requêtes pré-construites</h2>

Création d'un répertoire pour les requêtes SQL déjà enregistrées

~~~~~~~~~~~~~~
hadoop fs -mkdir /requests
~~~~~~~~~~~~~~
<h3>best_category</h3>

Création du fichier best_category.sql

~~~~~~~~~~~~~~
vi best_category.sql
~~~~~~~~~~~~~~

Contenu de best_category.sql

~~~~~~~~~~~~~~
SELECT T1.aisle, count(T3.order_id) from (select aisle_id, aisle from essou.aisles) as T1
JOIN essou.products as T2 ON (T1.aisle_id = T2.aisle_id)
JOIN essou.order_products_p as T3 ON (T2.product_id = T3.product_id) 
JOIN (select user_id, order_id from essou.orders_p where user_id = ${hiveconf:USERID}) as T4 ON (T3.order_id = T4.order_id) 
group by T1.aisle
order by count(T3.order_id) DESC limit 1;
~~~~~~~~~~~~~~

Insertion et utilisation de best_category.sql avec l'attribut USERID = 1

~~~~~~~~~~~~~~
hadoop fs -copyFromLocal best_category.sql /requests/best_category.sql
hive -hiveconf USERID=1 -f  <(hdfs dfs -cat /requests/best_category.sql) > best_category_output.txt
~~~~~~~~~~~~~~

<h3>best_hour</h3>

Création du fichier best_hour.sql

~~~~~~~~~~~~~~
vi best_hour.sql
~~~~~~~~~~~~~~

Contenu de best_hour.sql

~~~~~~~~~~~~~~
SELECT T1.order_hour_of_day
from essou.orders_p as T1
join essou.order_products_p as T2 on (T1.order_id = T2.order_id)
where T2.product_id = ${hiveconf:PRODUCTID}
group by T1.order_hour_of_day
order by count(T2.product_id) DESC limit 1;
~~~~~~~~~~~~~~

Insertion et utilisation de best_hour.sql avec l'attribut PRODUCTID=1

~~~~~~~~~~~~~~
hadoop fs -copyFromLocal best_hour.sql /requests/best_hour.sql
hive -hiveconf PRODUCTID=1 -f  <(hdfs dfs -cat /requests/best_hour.sql) > best_hour_output.txt
~~~~~~~~~~~~~~

<h3>orders_department_count</h3>
Création du fichier orders_department_count.sql

~~~~~~~~~~~~~~
vi orders_department_count.sql
~~~~~~~~~~~~~~

Contenu de orders_department_count.sql

~~~~~~~~~~~~~~
SELECT T1.department, count(T3.product_id)
from essou.departments as T1
join essou.products as T2 on (T1.department_id = T2.department_id)
join essou.order_products_p as T3 on (T2.product_id = T3.product_id)
where T3.order_id = ${hiveconf:ORDERID}
group by T1.department;
~~~~~~~~~~~~~~

Insertion et utilisation de orders_department_count.sql avec l'attribut ORDERID=2821986

~~~~~~~~~~~~~~
hadoop fs -copyFromLocal orders_department_count.sql /requests/orders_department_count.sql
hive -hiveconf ORDERID=2821986 -f  <(hdfs dfs -cat /requests/orders_department_count.sql) > orders_department_count_output.txt
~~~~~~~~~~~~~~





<h2>Base d'apprentissage pour le Machine Learning</h2>

Création du fichier machine_learnator.sql qui contient les données "applaties" de toutes les tables liées entre elles.

~~~~~~~~~~~~~~
vi machine_learnator.sql
~~~~~~~~~~~~~~

Contenu de machine_learnator.sql

~~~~~~~~~~~~~~
select T2.product_id, T2.product_name, T1.aisle, T3.department, T4.order_id, T4.add_to_cart_order, T4.reordered, T5.user_id, T5.order_number, T5.order_dow, T5.eval_set, T5.order_hour_of_day, T5.days_since_prior_order
from essou.aisles as T1 
join essou.products as T2 on (T1.aisle_id = T2.aisle_id)
join essou.departments as T3 on (T2.department_id = T3.department_id)
join essou.order_products_p as T4 on (T2.product_id = T4.product_id)
join essou.orders_p as T5 on (T4.order_id = T5.order_id);
~~~~~~~~~~~~~~

Insertion et utilisation de machine_learnator.sql.

~~~~~~~~~~~~~~
hadoop fs -copyFromLocal machine_learnator.sql /requests/machine_learnator.sql
hive -f  <(hdfs dfs -cat /requests/machine_learnator.sql) > machine_learnator.txt
~~~~~~~~~~~~~~
