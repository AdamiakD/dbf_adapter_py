import sys
import os
from typing_extensions import Self
import magic
import dbf
from tqdm import tqdm
import pandas as pd
from tkinter import filedialog
from tkinter import *
import dbfadapter
import time


class dbfadapter:

    def __init__(self, patch, cp_in, cp_out, sheet, dbffile, dataframe):
        self.patch = patch
        self.cp_in = cp_in
        self.cp_out = cp_out
        self.sheet = sheet
        self.dbffile = dbffile
        self.dataframe = dataframe

    def get_argv():
        patch = ''
        cp_in = ''
        cp_out = ''
        sheet = ''
        try:
            if sys.argv[1] != '':
                patch = sys.argv[1].replace('\"', '')
            for a in sys.argv:
                if '-cpin=' in a:
                    cp_in = a.replace('-cpin=', '')
                if '-cpout=' in a:
                    cp_out = a.replace('-cpout=', '')
                if '-sheet=' in a:
                    sheet = a.replace('-sheet=', '')
        except:
            return '', '', '', None
        return patch, cp_in, cp_out, sheet

    def get_sources(source):
        sourceFiles = []
        if source != '':
            if os.path.isdir(source.replace('\"', '')):
                os.chdir(source)
                sourceFiles = os.listdir(source)
            else:
                sourceFiles.append(source)

        return sourceFiles

    def detect_separator_in_csv(self):
        line = open(self.patch, "rb")
        head = line.readline()
        line.close()
        separators = '|;\t,'
        try:
            for sep in separators:
                if sep in head.decode():
                    break
            if sep == '':
                return f'Break work! No separtor found. Error: "{error}"'
        except Exception as error:
            return f'Break work! No separtor found. Error: "{error}"'

        print(f'Detected csv separator: "{sep}"')
        return sep

    def detect_encoding_in_csv(self):
        cp = magic.Magic(mime_encoding=True,
                         keep_going=False).from_file(self.patch)
        cp = cp.replace('windows-',
                        'cp').replace('iso-8859-1', 'cp1252').replace(
                            'iso-8859-5',
                            'cp1251').replace('iso-8859-7', 'cp1253').replace(
                                'iso-8859-8',
                                'cp1255').replace('utf-8', 'utf8')
        if cp[:7] == 'unknown':
            cp = ''

        print(f'Detected csv encoding: "{cp}"')
        return cp

    def read_from_excel(self):
        print(f'Reading data from "{os.path.basename(self.patch)}"')

        try:
            df = pd.read_excel(self.patch, dtype="str", sheet_name=self.sheet)
        except Exception as error:
            return f'Break work! Error: {error}'
        self.dataframe = df
        # return df

    def read_from_csv(self):
        sep = dbfadapter.detect_separator_in_csv(self.patch)

        if self.cp_in == '':
            return (f'Break work! Empty encoding for reading csv')

        print(
            f'Reading data from "{os.path.basename(self.patch)}" with encoding {self.cp_in}'
        )

        try:
            df = pd.read_csv(self.patch,
                             dtype="str",
                             engine='python',
                             delimiter=sep,
                             encoding=self.cp_in,
                             quoting=3)
        except Exception as error:
            return (f'Break work! Error: {error}')

        try:
            for c in range(len(df.columns)):
                df[df.columns[c]] = df[df.columns[c]].str.replace('""', '"~"')
                df[df.columns[c]] = df[df.columns[c]].str.strip('"')
                df[df.columns[c]] = df[df.columns[c]].str.replace(
                    '~"', '"').replace('"~', '"')
                df[df.columns[c]] = df[df.columns[c]].str.strip('~')
                df[df.columns[c]] = df[df.columns[c]].str.strip(' ')
        except Exception as error:
            return (f'Break work! Error: {error}')

        dffinal = {'': df}
        self.dataframe = dffinal
        # return dffinal

    def save_dbf(self):
        self.dbffile = os.path.splitext(self.patch)[0]
        if self.cp_out == 'utf8' or self.cp_out == '':
            return (
                f'Break work! The encoding "{self.cp_out}" is wrong for WRITING dbf.'
            )

        if 'DataFrame' in str(type(self.dataframe)):
            self.dataframe = {self.sheet: self.dataframe}
        sheetCnt = 0
        for s in self.dataframe:
            self.dbffile = (self.dbffile + '_' + s).strip('_') + '.dbf'
            dbfadapter.__write_dbf(self, s)
            sheetCnt += 1
        if sheetCnt > 1:
            print(f'The file contains more than one sheet...')

        print('Finished writing to dbf!')

    def __write_dbf(self, sheet):
        df = pd.DataFrame(self.dataframe[sheet])
        if len(df) > 0:
            df.fillna('', inplace=True)
            reccount = len(df)
            if len(df) < 1:
                return
            newDbfSpecs = ''
            fldList = []
            badcharnull = ' ?,:.!@#$%^&*()-;\n\t/\+\'`˝"Ï»¿'
            for c in range(len(df.columns)):
                col = df.columns[c]
                for char in badcharnull:
                    col = col.replace(char, '').replace('ß', 'ss')
                col = col[0:10]
                if col not in fldList:
                    fldList.append(col)
                else:
                    i = 1
                    while col in fldList:
                        i += 1
                        col = col[0:8] + str(i).rjust(2, '0')
                    fldList.append(col)
                maxFldLen = max(df.iloc[:, c].astype(str).apply(len))
                fldInfo = f'{col} C( {str(maxFldLen + 10)} );'
                if maxFldLen > 265:
                    fldInfo = f'{col} M;'
                newDbfSpecs += fldInfo
                # replace char \x81
                df[df.columns[c]] = df[df.columns[c]].str.replace(
                    chr(129), chr(252))

            print(
                f'Writing {reccount} records into "{os.path.basename(self.dbffile)}" with encoding {self.cp_out}'
            )
            if os.path.isfile(self.dbffile):
                os.remove(self.dbffile)
            try:
                dbfTable = dbf.Table(filename=self.dbffile,
                                     field_specs=newDbfSpecs,
                                     default_data_types=dict(C=dbf.Char),
                                     field_data_types=dict(C=dbf.Char),
                                     codepage=self.cp_out,
                                     dbf_type='vfp')
                dbfTable.open(dbf.READ_WRITE)
            except Exception as error:
                return (f'Break work! Error: {error}')

            with dbfTable:
                tq = tqdm(range(reccount), desc=f"Processing")
                for i in tq:
                    try:
                        row = df.loc[i]
                        dbfTable.append(tuple(row))
                    except Exception as error:
                        tq.close()
                        tq.clear()
                        dbfTable.close()
                        os.remove(self.dbffile)
                        return (
                            f'Break work! Try again with a different encoding of the dbf record\nEncoding {self.cp_out} error: {error}'
                        )

            sys.stdout.write('\x1b[1A')
            sys.stdout.write('\x1b[2K')


args = dbfadapter.get_argv()

#db = dbfadapter(args[0], args[1], args[2], args[3],'', '')
db = dbfadapter('n:/!projekty/dbfadapter_py/test/test.xlsx', 'cp1252',
                'cp1252', 'Sheet1', '', '')

if os.path.isfile(db.patch):
    fileExt = os.path.splitext(db.patch)[1].upper()
    xlsodsExt = ('.XLS', '.XLSX', '.XLSB', '.XLSM', '.ODS', '.ODT', '.ODF')
    csvExt = ('.CSV')
    if fileExt in xlsodsExt:
        if db.cp_out == '':
            db.cp_out = 'cp1252'
        db.read_from_excel()
        db.save_dbf()

    if fileExt in csvExt:
        if db.cp_in == '':
            db.cp_in = db.detect_encoding_in_csv(db.patch)
        if db.cp_out == '':
            db.cp_out = db.cp_in
        db.read_from_csv()
        db.save_dbf()

time.sleep(1)