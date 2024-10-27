from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("matura23").getOrCreate()

data_rates=spark.read.format('csv') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .option('sep', '\t') \
    .load('oceny.csv')
data_rates.cache()
data_rates.createOrReplaceTempView('rate_table')

data_gamers=spark.read.format('csv') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .option('sep', '\t') \
    .load('gracze.csv')
data_gamers.cache()

data_games=spark.read.format('csv') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .option('sep', '\t') \
    .load('gry.csv')
data_games.cache()


def task1():
    return spark.sql("""select id_gry, count(*) as num from rate_table group by id_gry order by num DESC""")


def task2():
    df_help_rates=data_rates.select('id_gry', 'ocena').groupBy('id_gry').avg('ocena')

    df_help_games = data_games.select('id_gry', 'kategoria') \
        .filter(data_games['kategoria']=='imprezowa')

    return df_help_games \
        .join(df_help_rates, df_help_games.id_gry==df_help_rates.id_gry) \
        .select(df_help_games.id_gry, df_help_games.kategoria, df_help_rates['avg(ocena)'].alias('srednia'))

def task3():
    return data_rates.select('id_gracza', 'stan', 'ocena') \
        .filter(data_rates['stan']!='posiada') \
        .groupBy('id_gracza').count()

def task4():
    df_junior=data_rates \
        .join(data_gamers, data_rates['id_gracza']==data_gamers['id_gracza']) \
        .join(data_games, data_rates['id_gry']==data_games['id_gry']) \
        .filter(data_gamers['wiek'] < 20) \
        .select(data_games['nazwa']) \
        .groupBy(data_games['nazwa']) \
        .count().alias('oceny') \
        .sort('count', ascending=False)


    df_senior = data_rates \
        .join(data_gamers, data_rates['id_gracza'] == data_gamers['id_gracza']) \
        .join(data_games, data_rates['id_gry'] == data_games['id_gry']) \
        .filter(20 <= data_gamers['wiek']) \
        .filter(data_gamers['wiek'] <=49) \
        .select(data_games['nazwa']) \
        .groupBy(data_games['nazwa']) \
        .count().alias('oceny') \
        .sort('count', ascending=False)


    df_veteran=data_rates \
        .join(data_gamers, data_rates['id_gracza']==data_gamers['id_gracza']) \
        .join(data_games, data_rates['id_gry']==data_games['id_gry']) \
        .filter(data_gamers['wiek'] > 49) \
        .select(data_games['nazwa']) \
        .groupBy(data_games['nazwa']) \
        .count() \
        .sort('count', ascending=False)

    df_junior_top=df_junior.select('nazwa', 'count') \
        .filter(df_junior['count']==df_junior.first()['count'])

    df_senior_top=df_senior.select('nazwa', 'count') \
        .filter(df_senior['count']==df_senior.first()['count'])

    df_veteran_top=df_veteran.select('nazwa', 'count') \
        .filter(df_veteran['count']==df_veteran.first()['count'])

    return df_junior_top.union(df_senior_top.union(df_veteran_top))



# answer: 58
# result1=task1()
# result1.show(5)

# result2=task2()
# result2.createOrReplaceTempView('result_task2')
# spark.sql("""select id_gry, kategoria, round(srednia, 2) from result_task2""").show()

# answer: 819
# result3=task3()
# print(len(result3.collect()))

# result4=task4()
# result4.show()

spark.stop()
