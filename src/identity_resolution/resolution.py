from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("IdentityResolution").getOrCreate()

# Define S3 paths for the two identities
data_path_consumer = "s3a://my-bucket/test-data/source/"
data_path_partner = "s3a://my-bucket/test-data/source/"

# Read data from S3 paths (assuming Parquet format, adjust if needed)
df_consumer = spark.read.parquet(data_path_consumer).limit(1000)
# df_partner = spark.read.parquet(data_path_partner)

# Register as temporary views (tables)
df_consumer.createOrReplaceTempView("consumer_identity")
df_consumer.createOrReplaceTempView("partner_identity")

# Write each CTE to a separate CSV file and print results
consumer_data = spark.sql('''
    SELECT id AS omni_id,
          'email' AS id_type,
          SHA2(trim(email),256) AS id_value
    FROM consumer_identity
    UNION ALL
    SELECT id AS omni_id,
            'name' AS id_type,
          lower(name) AS id_value
    FROM consumer_identity
    UNION ALL
    SELECT id AS omni_id,
            'address' AS id_type,
          lower(address) AS id_value
    FROM consumer_identity
    UNION ALL
    SELECT id AS omni_id,
            'phone' AS id_type,
          phone_number AS id_value
    FROM consumer_identity
    ''')
consumer_data.write.csv("s3a://my-bucket/output/consumer_data/", mode="overwrite", header=True)
consumer_data.createOrReplaceTempView("consumer_cte")

provider_data = spark.sql('''
    SELECT id AS account_id,
          email,
          get_json_object(name,'$.first_name') AS first_name,
          get_json_object(name,'$.last_name') AS last_name,
          get_json_object(address,'$.address1') AS address1,
          get_json_object(address,'$.city') AS city,
          get_json_object(address,'$.state') AS state,
          get_json_object(address,'$.zip') AS zip,
          phone_number AS cell_phone
    FROM partner_identity''')
provider_data.write.csv("s3a://my-bucket/output/provider_data/", mode="overwrite", header=True)


provider_standardized = spark.sql('''
    SELECT  account_id as partner_id,
            SHA2(trim(email),256) as email,
            CASE WHEN regexp_count(trim(lower(first_name)), '[^a-z]') = 0 THEN trim(lower(first_name))
                ELSE split(trim(lower(first_name)),'[^a-z]')[0] 
            END AS first_name,
            CASE WHEN regexp_count(trim(lower(last_name)), '[^a-z]') = 0 THEN trim(lower(last_name))
                ELSE split(trim(lower(last_name)),'[^a-z]')[1] 
            END AS last_name,
            regexp_replace(trim(lower(address1)),'[^a-z0-9]','') AS address1,
            trim(lower(city)) AS city,
            trim(lower(state)) AS state,
            substr(trim(lower(zip)),1,5) AS zip,
            CASE WHEN length(trim(lower(cell_phone))) = 10 AND regexp_count(cell_phone, '[^0-9]') = 0 THEN cell_phone
                ELSE COALESCE(regexp_replace(regexp_substr(cell_phone, '[0-9]{3}[-.)]{1}[0-9]{3}[-.]{1}[0-9]{4}') ,'[^0-9]',''), 
                        regexp_replace(cell_phone,'[^0-9]', ''))
            END AS cell_phone
    FROM (
        SELECT id AS account_id,
              email,
              get_json_object(name,'$.first_name') AS first_name,
              get_json_object(name,'$.last_name') AS last_name,
              get_json_object(address,'$.address1') AS address1,
              get_json_object(address,'$.city') AS city,
              get_json_object(address,'$.state') AS state,
              get_json_object(address,'$.zip') AS zip,
              phone_number AS cell_phone
        FROM partner_identity
    )
''')
provider_standardized.write.csv("s3a://my-bucket/output/provider_standardized/", mode="overwrite", header=True)
provider_standardized.createOrReplaceTempView("provider_cte")

setup_data = spark.sql("""
-- SELECT a.omni_id, b.partner_id, 'email' as match_type
-- FROM consumer_cte a
-- JOIN provider_cte b
-- ON a.id_value = b.email
-- WHERE a.id_type = 'email'
-- UNION ALL
-- SELECT  omni_id, b.partner_id, 'name' as match_type
-- FROM consumer_cte a
-- JOIN provider_cte b
-- ON b.first_name = get_json_object(a.id_value,'$.first_name') AND b.last_name = get_json_object(a.id_value,'$.last_name') 
-- WHERE a.id_type = 'name'
-- UNION ALL
SELECT  omni_id, b.partner_id, b.value, 'address' as match_type
FROM consumer_cte a
JOIN provider_cte b
ON position(regexp_replace(get_json_object(a.id_value,'$.address1'),' ','') IN b.address1) > 0 AND
   ((b.city = get_json_object(a.id_value,'$.city') AND
    b.state = get_json_object(a.id_value,'$.state')) OR
    (b.zip = get_json_object(a.id_value,'$.zip')))
WHERE a.id_type = 'address'
-- UNION ALL
-- SELECT  omni_id, b.partner_id, 'phone' as match_type
-- FROM consumer_cte a
-- JOIN provider_cte b
-- ON (length(b.cell_phone) = 10 AND b.cell_phone = a.id_value) OR (position(a.id_value IN b.cell_phone) > 0)
-- WHERE a.id_type = 'phone'
""")
setup_data.write.csv("s3a://my-bucket/output/setup_data/", mode="overwrite", header=True)
setup_data.createOrReplaceTempView("setup_cte")

detail_data = spark.sql("""
    select omni_id, partner_id, ARRAY_JOIN(COLLECT_LIST(match_type),',') as match_elements
    FROM setup_cte
    GROUP BY omni_id, partner_id
""")

detail_data.write.csv("s3a://my-bucket/output/detail_data/", mode="overwrite", header=True)

detail_data.createOrReplaceTempView("detail_cte")

# Final result
result = spark.sql('''
SELECT match_elements,
       COUNT(DISTINCT omni_id) AS omni_id_count,
       COUNT(DISTINCT partner_id) AS partner_id_count
FROM detail_cte
GROUP BY 1
''')
result.write.csv("s3a://my-bucket/output/final_result/", mode="overwrite", header=True)
result.show()