from pyspark import SparkConf, SparkContext, StorageLevel

def GeoLocation(line):
    dic = {}
    dic['location'] = line[18:25]
    dic['name'] = line[226:316].strip()
    dic['id'] = line[8:11]
    return dic

def PopulationColorado(line):
    dic = {}
    dic['location'] = line[4]
    dic['male'] = int(line[6])
    dic['female'] = int(line[30])
    return dic

def countyPopulation(x):
    dic = {}
    dic['nameCounty'] = x[1][0]['name']
    dic['male'] = x[1][1]['male']
    dic['female'] = x[1][1]['female']
    return dic

def countWomen(dic):
    total_people = dic['male'] + dic['female']
    return (dic['nameCounty'], dic['female']/total_people)

    

def main(sc, filename_Geo, filename_People, pathResult):
    
    location_rdd = sc.textFile(filename_Geo).setName('County Location')
    location_rdd.persist(StorageLevel.MEMORY_ONLY)
    location_rdd = location_rdd.map(GeoLocation).filter(lambda x: x['id'] == '050')
    print(location_rdd.count())

    population_rdd = sc.textFile(filename_People).setName('County Population')
    population_rdd.persist(StorageLevel.MEMORY_ONLY)
    population_rdd = population_rdd.map(lambda x: x.split(',')).map(PopulationColorado)
    print(population_rdd.count())

    locationPair = location_rdd.map(lambda x: (x['location'], x))
    populationPair = population_rdd.map(lambda x: (x['location'], x))
    join_rdd = locationPair.join(populationPair)
    
    moreWomen_rdd = join_rdd.map(countyPopulation).map(countWomen)
    moreWomen = moreWomen_rdd.map(lambda x: (x[1], x[0])).sortByKey(ascending = False)
    moreWomen.saveAsTextFile(pathResult)


if __name__ == "__main__":

   appName = "More Female by County"
   fileNameGeo = "Localitation.sf1"
   fileNamePeople = "ColoradoPeople.sf1"
   pathResult = "WomenInColorado"
   conf = SparkConf().setAppName(appName)
   conf = conf.setMaster("local[*]")
   sc = SparkContext(conf=conf)
   main(sc, fileNameGeo, fileNamePeople, pathResult)