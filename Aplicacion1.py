from pyspark import SparkConf, SparkContext
import re

def removePunctuation(text):
  return re.sub(r'[^A-Za-zÀ-ÿ0-9 ]', '', text).lower().strip()


def main(sc, filename, minWords):

    quevedoRDD = sc.textFile(filename)
    print('El buscón de Quevedo tiene {} lineas.'.format(quevedoRDD.count()))

    quevedoRDD = quevedoRDD.map(removePunctuation)
    quevedoWordsRDD = quevedoRDD.flatMap(lambda x: x.split(' '))
    wordsRDD = quevedoWordsRDD.filter(lambda x: len(x) > 0)
    print('El buscón de Quevedo tiene {} palabras.'.format(wordsRDD.count()))

    freqRDD = wordsRDD.map(lambda x: (x,1)).reduceByKey(lambda x, y: x + y)
    maxWords = freqRDD.filter(lambda x: x[1] > minWords)
    output = sorted(maxWords.collect(), key = lambda x: x[1], reverse = True)

    for word in output:
      print(word)


if __name__ == "__main__":

   appName = "Max Word of El Buscon"
   fileName = "El_buscón-Quevedo.txt"
   minWords = 150
   conf = SparkConf().setAppName(appName)
   conf = conf.setMaster("local[*]")
   sc = SparkContext(conf=conf)
   main(sc, fileName, minWords)