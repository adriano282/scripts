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
    
    def today = new Date().clearTime()
    def nextSemester = today + 180
    def createTableStmt = """
    CREATE TABLE bigtable (
        my_date        timestamp,
        id             bigserial,
        idref          INT,
        col4           VARCHAR,
        col5           VARCHAR,
        col6           VARCHAR,
        col7           VARCHAR,
        col8           VARCHAR
    ) PARTITION BY RANGE (my_date);
    """ 

    println "#### Create table bigtable ####"
    sql.execute(createTableStmt)
    println "Executed >>> " + createTableStmt


    println "#### Create table partitions for bigtable ####\n"
    def qtdePartitions = 0
    def timestampFormatted
    def forNameFormatted
    def rowsPerPartitions = 5_082_295
    def startTimeInsert
    today.upto(nextSemester) {

        timestampFormatted = it.format('yyyy-MM-dd 00:00:00')
        forNameFormatted = it.format('yyyyMMdd')

        println "\t create partition ..."
        sql.execute("""
            CREATE TABLE big_table_$forNameFormatted PARTITION OF bigtable
                FOR VALUES FROM ('$timestampFormatted') TO ('${(it + 1).format('yyyy-MM-dd 00:00:00')}');
        """.toString())

        println "\t create index for my_date column ..."
        
        sql.execute("""
            CREATE INDEX big_table_idx_$forNameFormatted ON big_table_$forNameFormatted USING BRIN (my_date) WITH (pages_per_range = 128);
        """.toString())

        println "\t insert $rowsPerPartitions rows in partition ..."
        /*sql.execute("""
            insert into bigtable (my_date, id, idref, col4, col5, col6, col7, col8)
            select '$timestampFormatted', 1, generate_series(1, $rowsPerPartitions), 'abc', 'abc', 'abc', 'abc', 'abc';
        """.toString())
        */
        startTimeInsert = new Date()
        sql.execute("""
            insert into bigtable (my_date, id, idref, col4, col5, col6, col7, col8)
            select generate_series('${it.format("yyyy-MM-dd 00:00:00")}'::timestamp,'${it.format("yyyy-MM-dd 23:59:59")}'::timestamp,'17 millisecond'::interval), 1, 1, 'abc', 'abc', 'abc', 'abc', 'abc';
        """.toString())
        println "Insert of $rowsPerPartitions rows in ${TimeCategory.minus(new Date(), startTimeInsert)}"

        qtdePartitions++

        println "Partition for $it created and loaded\n"
    }

    println "$qtdePartitions partitions created"

    println "Total rows: ${rowsPerPartitions * qtdePartitions}"
} finally    {
    sql.close()
    println "done"
}
