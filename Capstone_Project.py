df_subscriber.write.format("redshift")\
.option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\#change jdbc
.option("dbtable", "test.subscriber")\#test.mathi valriable j cha tai
.option("driver","com.amazon.redshift.jdbc42.Driver")\
.option("user", "admin").option("password", "Nepal123")\#change password
.option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
.option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin")\#change role
.mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZ5Z3KDW5NOTW2BK3")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
claims_data=spark.read.option("header","true").json("s3://july17/capstone_project/claims.json")
claims_data.show()

have_nulls = claims_data.dropna().count() < claims_data.count()
if have_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")

claims_data.write.format("redshift").option("url", "jdbc:redshift:redshift-cluster-1.cbm63ss89hj1.us-east-1.redshift.amazonaws.com:5439/dev").\
    option("dbtable", "Test.claims_data").\
    option("aws_iam_role", "arn:aws:iam::682487324090:user/HelloWorld").\
    option("driver", "com.amazon.redshift.jdbc42.Driver").\
    option("tempdir", "s3a://july17/output/1").\
    option("user", "admin").\
    option("password", "Test456").mode("overwrite").save()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZ5Z3KDW5NOTW2BK3")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
subscriber_records=spark.read.option("header","true").csv("s3://july17/capstone_project/subscriber.csv")
subscriber_records.show()

have_nulls = subscriber_records.dropna().count() < subscriber_records.count()
if have_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")

subscriber_records=subscriber_records.withColumnRenamed("sub _id", "sub_id")
subscriber_records=subscriber_records.withColumnRenamed("Zip Code", "ZipCode")

from pyspark.sql.functions import col

null_sums = []

for col_name in subscriber_records.columns:
null_sum = subscriber_records.filter(col(col_name).isNull()).count()
null_sums.append((col_name, null_sum))

print("Sum of null values in each column:")
for col_name, null_sum in null_sums:
print(f'Column "{col_name}": {null_sum}')

subscriber_records = subscriber_records.na.fill("NA", subset=["first_name"])
subscriber_records = subscriber_records.na.fill("NA", subset=["Phone"])
subscriber_records = subscriber_records.na.fill("NA", subset=["Subgrp_id"])
subscriber_records = subscriber_records.na.fill("NA", subset=["Elig_ind"])

subscriber_records.write.format("redshift").option("url", "jdbc:redshift:redshift-cluster-1.cbm63ss89hj1.us-east-1.redshift.amazonaws.com:5439/dev").\
    option("dbtable", "Test.subscriber_records").\
    option("aws_iam_role", "arn:aws:iam::682487324090:user/HelloWorld").\
    option("driver", "com.amazon.redshift.jdbc42.Driver").\
    option("tempdir", "s3a://july17/output/1").\
    option("user", "admin").\
    option("password", "Test456").mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZ5Z3KDW5NOTW2BK3")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
subgroup_records = spark.read.option("header", "true").csv("s3://july17/capstone_project/subgroup.csv")
subgroup_records.show()

have_nulls = subgroup_records.dropna().count() < subgroup_records.count()
if have_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")

subgroup_records.write.format("redshift").option("url", "jdbc:redshift:redshift-cluster-1.cbm63ss89hj1.us-east-1.redshift.amazonaws.com:5439/dev").\
    option("dbtable", "Test.subgroup_records").\
    option("aws_iam_role", "arn:aws:iam::682487324090:user/HelloWorld").\
    option("driver", "com.amazon.redshift.jdbc42.Driver").\
    option("tempdir", "s3a://july17/output/1").\
    option("user", "admin").\
    option("password", "Test456").mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZ5Z3KDW5NOTW2BK3")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
patient_records=spark.read.option("header","true").csv("s3://july17/capstone_project/Patient_records.csv")
patient_records.show()

have_nulls = patient_records.dropna().count() < patient_records.count()
if have_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")

from pyspark.sql.functions import col

null_sums = []

for col_name in patient_records.columns:
null_sum = patient_records.filter(col(col_name).isNull()).count()
null_sums.append((col_name, null_sum))

print("Sum of null values in each column:")
for col_name, null_sum in null_sums:
print(f'Column "{col_name}": {null_sum}')

patient_records = patient_records.na.fill("NA", subset=["Patient_name"])
patient_records = patient_records.na.fill("NA", subset=["patient_phone"])

patient_records.show()

patient_records.write.format("redshift").option("url", "jdbc:redshift:redshift-cluster-1.cbm63ss89hj1.us-east-1.redshift.amazonaws.com:5439/dev").\
    option("dbtable", "Test.patient_records").\
    option("aws_iam_role", "arn:aws:iam::682487324090:user/HelloWorld").\
    option("driver", "com.amazon.redshift.jdbc42.Driver").\
    option("tempdir", "s3a://july17/output/1").\
    option("user", "admin").\
    option("password", "Test456").mode("overwrite").save()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZ5Z3KDW5NOTW2BK3")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
grpsubgrp_data=spark.read.option("header","true").csv("s3://july17/capstone_project/grpsubgrp.csv")
grpsubgrp_data.show()


have_nulls = grpsubgrp_data.dropna().count() < grpsubgrp_data.count()
if have_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")

grpsubgrp_data.write.format("redshift").option("url", "jdbc:redshift:redshift-cluster-1.cbm63ss89hj1.us-east-1.redshift.amazonaws.com:5439/dev").\
    option("dbtable", "Test.grpsubgrp_data").\
    option("aws_iam_role", "arn:aws:iam::682487324090:user/HelloWorld").\
    option("driver", "com.amazon.redshift.jdbc42.Driver").\
    option("tempdir", "s3a://july17/output/1").\
    option("user", "admin").\
    option("password", "Test456").mode("overwrite").save()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZ5Z3KDW5NOTW2BK3")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
group_data = spark.read.option("header", "true").csv("s3://july17/capstone_project/group.csv")
group_data.show()

have_nulls = group_data.dropna().count() < group_data.count()
if have_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")

group_data.write.format("redshift").option("url", "jdbc:redshift:redshift-cluster-1.cbm63ss89hj1.us-east-1.redshift.amazonaws.com:5439/dev"). \
    option("dbtable", "Test.group_data"). \
    option("aws_iam_role", "arn:aws:iam::682487324090:user/HelloWorld"). \
    option("driver", "com.amazon.redshift.jdbc42.Driver"). \
    option("tempdir", "s3a://july17/output/1"). \
    option("user", "admin"). \
    option("password", "Test456").mode("overwrite").save()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZ5Z3KDW5NOTW2BK3")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
disease_data = spark.read.option("header", "true").csv("s3://july17/capstone_project/disease.csv")
disease_data.show()

have_nulls = disease_data.dropna().count() < disease_data.count()
if have_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")


disease_data.write.format("redshift").option("url", "jdbc:redshift:redshift-cluster-1.cbm63ss89hj1.us-east-1.redshift.amazonaws.com:5439/dev"). \
    option("dbtable", "Test.disease_data"). \
    option("aws_iam_role", "arn:aws:iam::682487324090:user/HelloWorld"). \
    option("driver", "com.amazon.redshift.jdbc42.Driver"). \
    option("tempdir", "s3a://july17/output/1"). \
    option("user", "admin"). \
    option("password", "Test456").mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZ5Z3KDW5NOTW2BK3")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
hospital_data=spark.read.option("header","true").csv("s3://july17/capstone_project/disease.csv")
hospital_data.show()

have_nulls = hospital_data.dropna().count() <hospital_data.count()
if have_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")


hospital_data.write.format("redshift").option("url", "jdbc:redshift:redshift-cluster-1.cbm63ss89hj1.us-east-1.redshift.amazonaws.com:5439/dev"). \
    option("dbtable", "Test.hospital_data"). \
    option("aws_iam_role", "arn:aws:iam::682487324090:user/HelloWorld"). \
    option("driver", "com.amazon.redshift.jdbc42.Driver"). \
    option("tempdir", "s3a://july17/output/1"). \
    option("user", "admin"). \
    option("password", "Test456").mode("overwrite").save()


