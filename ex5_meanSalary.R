library(SparkR)

# Initialize SparkContext and SQLContext
sc <- sparkR.init(appName="MeanSalary")
sqlContext <- sparkRHive.init(sc)

# Read hh-vacs to Spark data frame
print('Read data')
hh <- read.json(sqlContext, "hh-vacs")
printSchema(hh)

# Register this data frame as a table.
registerTempTable(hh, "hh")

# Helper method to get vacancy depending on specializations.name
getVacancyBySpecialization <- function(spec) {
  specTemplate <- paste0("'%", spec, "%'")

  sql(sqlContext, paste(
    "SELECT",
      "COALESCE(address.city, area.name) AS city,",
      "COALESCE((salary.`from` + salary.to) / 2, salary.`from`, salary.to) AS salary,",
      "salary.currency AS currency",
    "FROM hh LATERAL VIEW explode(specializations) name AS spec", 
    "WHERE spec.name LIKE", specTemplate,
    "AND (salary.`from` IS NOT NULL OR salary.to IS NOT NULL)"
  ))
}

# Picking programmers, designers, and sysadmins from hh table
programmers <- getVacancyBySpecialization('Программирование')
sysadmins <- getVacancyBySpecialization('Системный администратор')
designers <- getVacancyBySpecialization('Дизайн')

# Since original data includes multicurrency ("BYR" "EUR" "KZT" "RUR" "UAH" "USD")
# it needs to be converted into single currency (US dollars in my case) to make it
# possible to calculate the avarage.
#
# Get realtime exchange rate is not that straight forward task so I just hardcoded
# quotes against dollar here (that's for 5 April 2016):

fx <- data.frame(
  BYR = 0.000049,
  KZT = 0.0029, 
  RUR = 0.014,
  UAH = 0.038,
  EUR = 1.14
)

# Helper method to convert salary to USD and keep it in usdSalary column
collectUsdSalaries <- function(vacancies) {
  localDf <- collect(vacancies)

  localDf$usdSalary <- apply(localDf, 1, function(line) {
    salary <- as.numeric(line[2])
    currency <- line[3]
    if( currency != 'USD' ){
      salary <- salary * fx[[currency]]
    }
    salary
  }) 

  localDf
}

# Figure out salary in USD
programmers.local <- collectUsdSalaries(programmers)
designers.local <- collectUsdSalaries(designers)
sysadmins.local <- collectUsdSalaries(sysadmins)

# Aggregate mean salaries by cities
programmers.meanSalary <- aggregate(programmers.local['usdSalary'], list(city=programmers.local$city), mean)
designers.meanSalary <- aggregate(designers.local['usdSalary'], list(city=designers.local$city), mean)
sysadmins.meanSalary <- aggregate(sysadmins.local['usdSalary'], list(city=sysadmins.local$city), mean)


print('Средняя зарплата программиста по городам')
setNames(programmers.meanSalary, c('Город', 'Зарплата $'))

print('Средняя зарплата дизайнера по городам')
setNames(designers.meanSalary, c('Город', 'Зарплата $'))

print('Средняя зарплата сисадмина по городам')
setNames(sysadmins.meanSalary, c('Город', 'Зарплата $'))


# Stop the SparkContext
sparkR.stop()
