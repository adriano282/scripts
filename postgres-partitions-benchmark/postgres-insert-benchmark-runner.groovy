import groovy.sql.Sql
import groovy.time.TimeDuration
import groovy.time.TimeCategory

def url = 'jdbc:postgresql://localhost:5432'
def database = 'benchmark'
def user = 'postgres'
def password = 'postgres'
def driver = 'org.postgresql.Driver'
def sql = Sql.newInstance("$url/$database", user, password, driver)

try {

    /*def insertThroughPartition = 
    """
    insert into big_table_DATE(my_date, id, idref, col4, col5, col6, col7, col8)
    values (?, ?, ?, ?, ?, ?, ?, ?)
    """*/

    def insertThroughParentTable = 
    """
    insert into bigtable(my_date, id, idref, col4, col5, col6, col7, col8)
    values (?, ?, ?, ?, ?, ?, ?, ?)
    """

    Date myDateColumn = new Date()
    String otherColumns = "Some important value"
    
    int amountOfInserts = 2960;
    TreeSet<Integer> durationInMillisPerInsert = new TreeSet<Integer>()
    TimeDuration duration;

    //def formattedDateForPartitionName
    //def formatedPartitionInsert 

    Date startTimeOneInsert
    Date startTimeAllInserts = new Date()
    (1..amountOfInserts).each {
        
        //formattedDateForPartitionName = myDateColumn.format('yyyyMMdd')
        //formatedPartitionInsert = insertThroughPartition.replaceAll("DATE", formattedDateForPartitionName)

        startTimeOneInsert = new Date()
        //sql.executeInsert formatedPartitionInsert, [myDateColumn.toTimestamp(), it, it, otherColumns, otherColumns, otherColumns, otherColumns, otherColumns];
        sql.executeInsert insertThroughParentTable, [myDateColumn.toTimestamp(), it, it, otherColumns, otherColumns, otherColumns, otherColumns, otherColumns];
        duration = TimeCategory.minus( new Date(), startTimeOneInsert )

        println duration
        durationInMillisPerInsert.add(duration.toMilliseconds())

        if (it % 20 == 0) {
            myDateColumn += 1
        }
        
    }

    def totalDuration = TimeCategory.minus( new Date(), startTimeAllInserts );
    println "Total time execution time: ${totalDuration}"

    TreeMap<Integer, Integer> percentilResultInMillis = new TreeMap<Integer, Integer>()

    def percentil
    durationInMillisPerInsert.eachWithIndex { item, index -> 

        if (durationInMillisPerInsert.higher(item) != null) {

            percentil = ((index +1)/durationInMillisPerInsert.size()).setScale(2, BigDecimal.ROUND_HALF_UP).toDouble()

            if (percentil <= 0.25)
                percentilResultInMillis.put(BigDecimal.valueOf(0.25), item)
            else if (percentil <= 0.5)
                percentilResultInMillis.put(BigDecimal.valueOf(0.50), item)
            else if (percentil <= 0.75)
                percentilResultInMillis.put(BigDecimal.valueOf(0.50), item)
            else if (percentil <= 0.90)
                percentilResultInMillis.put(BigDecimal.valueOf(0.90), item)
            else if (percentil <= 0.95)
                percentilResultInMillis.put(BigDecimal.valueOf(0.95), item)
            else if (percentil <= 0.99)
                percentilResultInMillis.put(BigDecimal.valueOf(0.99), item)
        }
        else {
            percentilResultInMillis.put(BigDecimal.valueOf(1), item)
        }
    }

    println "\nPercentil Result: "
    percentilResultInMillis.each {
        println String.format("%.2f - %d millis", it.key, it.value)
    }

    println "\nMedium time: ${totalDuration.toMilliseconds()/amountOfInserts} millis"


} finally {
    sql.close()
    println "sql connection closed"
}

