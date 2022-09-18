import dbfadapter
import time
import os
from tkinter import filedialog
from tkinter import *

da = dbfadapter
args = da.dbfadapter.get_argv()

patch = args[0]
cpin = args[1]
cpout = args[2]
sheet = args[3]

sources = da.dbfadapter.get_sources(patch)

if sources == '':
    tk = Tk()
    tk.withdraw()
    sourceFiles = filedialog.askopenfilenames(
        title='Select excel file(s)...',
        filetypes=[
            ("Supported files",
             r"*.xls  *.xlsx  *.xlsb  *.xlsm  *.ods  *.odt  *.odf  *.csv"),
            ("All files", "*.*")
        ])

for file in sources:
    if os.path.isfile(file):
        dbffile = os.path.abspath(file)
        fileExt = os.path.splitext(file)[1].upper()
        xlsodsExt = ('.XLS', '.XLSX', '.XLSB', '.XLSM', '.ODS', '.ODT', '.ODF')
        csvExt = ('.CSV')
        if fileExt in xlsodsExt:
            if cpout == '':
                cpout = 'cp1252'
            data = da.dbfadapter.read_from_excel(sourceFile=file, sheet=sheet)
            da.dbfadapter.save_dbf(dataframe=data,
                                   dbffile=file,
                                   encoding=cpout)

        if fileExt in csvExt:
            if cpin == '':
                cpin = da.dbfadapter.detect_encoding_in_csv(file)
            if cpout == '':
                cpout = cpin
            data = da.dbfadapter.read_from_csv(sourceFile=file, cp_in=cpin)
            da.dbfadapter.save_dbf(dataframe=data,
                                   dbffile=file,
                                   encoding=cpout)

time.sleep(1)