import sqlite3
'''
A python 3 script to insert csv tables into a sqlite3 database
Author : Aditya Ambati ambati@stanford.edu

'''
def WriteDataDB(conInstance, tableName, csvFile):
    con = sqlite3.connect(conInstance)
    cur = con.cursor()
    Header = ''
    TempData = []
    Lines = 0
    LineBuffer = 0
    with open(csvFile) as fileIn:
        for n, line in enumerate(fileIn):
            lineParse = tuple(line.strip().split(','))
            if n > 0:
                ValType="?,"*len(lineParse)
                if Lines == 10000:
                    LineBuffer += 10000
                    TempData.append(lineParse)
                    print("EXECUTING DB COMMANDS {} LINES".format(LineBuffer))
                    try:
                        cur.executemany("INSERT INTO " + tableName + " "+Header+ " VALUES ("+ValType.strip(',')+");", TempData)
                        con.commit()
                        TempData = []
                        Lines = 0
                    except:
                        con.rollback()
                        pass
                else:
                    Lines += 1
                    TempData.append(lineParse)
            else:
                Header = str(lineParse)
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS "+ tableName +" "+Header+";")
        if len(TempData) > 0:
            try:
                cur.executemany("INSERT INTO " + tableName + " "+Header +
                                " VALUES ("+ValType.strip(',')+");", TempData)
                con.commit()
                TempData = []
            except:
                con.rollback()
                pass

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-DB', help = 'Full path to a database file, if not present it will be created', required = True)
    parser.add_argument('-TABLE', help ='Table name in database')
    parser.add_argument('-CSV', help='CSV file to write to DB')
    args = parser.parse_args()
    conInstance = args.DB
    tableName = args.TABLE
    csvFile = args.CSV
    print(args)
    WriteDataDB(
        conInstance=conInstance,
        tableName=tableName,
        csvFile=csvFile)


if __name__ == '__main__':
    main()


# WriteDataDB(conInstance="/media/labcomp/HDD2/NMDA_GWAS_2018/KIR_IMPUTATION/KIRDatabase.DB",
# tableName="PMRA_PLATES_77_to_113",
# csvFile="/media/labcomp/HDD2/NMDA_GWAS_2018/KIR_IMPUTATION/Plates77_to_113/imputations.csv")
# WriteDataDB(conInstance="/media/labcomp/HDD2/NMDA_GWAS_2018/KIR_IMPUTATION/KIRDatabase.DB",
#             tableName="EUR_NMDA_GERA_DISC",
#             csvFile="/media/labcomp/HDD2/NMDA_GWAS_2018/KIR_IMPUTATION/EUR_DISC_GERA/imputations.csv")
# def read_from_db(c):
#     c.execute('SELECT * FROM PMRA_PLATES_77_to_113')
#     data = c.fetchall()
#     #print(data)
#     out = [row for row in data]
#     return out


# con = sqlite3.connect(
#     "/media/labcomp/HDD2/NMDA_GWAS_2018/KIR_IMPUTATION/KIRDatabase.DB")
# cur = con.cursor()

# test=read_from_db(cur)
