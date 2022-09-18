import sys
import os
import dbf
import pandas as pd
from tkinter import filedialog
from tkinter import messagebox
from tkinter import *
from tqdm import tqdm
import time
import magic
import PySimpleGUI as psg
import dbfadapter as dbfs

default_read_encoding = ''
default_write_encoding = ''


class dbfadapter:

    def __init__(self, finalfile):
        self.finalfile = finalfile

    def detect_separator_in_csv(sourcefile):
        line = open(sourcefile, "rb")
        head = line.readline()
        line.close()
        separators = '|;\t,'
        try:
            for sep in separators:
                if sep in head.decode():
                    break
        except Exception as error:
            print(f'Break work! No separtor found. Error: "{error}"')
        print(f'Detected csv separator: "{sep}"')
        return sep

    def detect_encoding_in_csv(sourcefile):
        cp = magic.Magic(mime_encoding=True,
                         keep_going=False).from_file(sourcefile)
        cp = cp.replace('windows-',
                        'cp').replace('iso-8859-1', 'cp1252').replace(
                            'iso-8859-5',
                            'cp1251').replace('iso-8859-7', 'cp1253').replace(
                                'iso-8859-8',
                                'cp1255').replace('utf-8', 'utf8')
        if cp[:7] == 'unknown':
            cp = dbfadapter.combo_codepages_list(
                f'The encoding "{cp}" is wrong for READING csv. Enter the correct:'
            )
        print(f'Detected csv encoding: "{cp}"')
        return cp

    def read_from_excel(sourcefile, cp_out, sheet=None):
        print(f'Reading data from "{os.path.basename(sourcefile)}"')
        try:
            excelFile = pd.read_excel(sourcefile,
                                      dtype="str",
                                      sheet_name=sheet)
        except Exception as error:
            print(f'Break work! Error: {error}')
            sys.exit(1)
        if 'DataFrame' in str(type(excelFile)):
            excelFile = {sheet: excelFile}
        sheetCnt = 0
        for sheet in excelFile:
            dbfa.finalfile = os.path.splitext(
                sourcefile)[0] + '_' + sheet + '.dbf'
            df = pd.DataFrame(excelFile[sheet])
            if len(df) > 0:
                if cp_out == '':
                    cp_out = 'cp1252'
                dbfadapter.save_dbf(df, encoding=cp_out)
                sheetCnt += 1
        if sheetCnt > 1:
            dbfadapter.messages("The file contains more than one sheet...")

    def read_from_csv(sourcefile, cp_in, cp_out):
        dbfa.finalfile = os.path.splitext(sourcefile)[0] + '.dbf'

        sep = dbfadapter.detect_separator_in_csv(sourcefile)
        if cp_in == '':
            cp_in = dbfadapter.detect_encoding_in_csv(sourcefile)

        print(
            f'Reading data from "{os.path.basename(sourcefile)}" with encoding {cp_in}'
        )

        try:
            df = pd.read_csv(
                sourcefile,
                dtype="str",
                engine='python',
                delimiter=sep,
                # sep=f'"*[{sep}]"*',
                encoding=cp_in,
                # doublequote=True,
                quoting=3)
        except Exception as error:
            print(f'Break work! Error: {error}')
            sys.exit(1)

        # The default quoting option in read_csv generates incorrect reads inside strings.
        # It is set to QUOTE_NONE (3) so it leaves all quotes. I am now trimming the external quotes....
        # Below removing quotation marks and spaces:
        for c in range(len(df.columns)):
            df[df.columns[c]] = df[df.columns[c]].str.replace('""', '"~"')
            df[df.columns[c]] = df[df.columns[c]].str.strip('"')
            df[df.columns[c]] = df[df.columns[c]].str.replace('~"',
                                                              '"').replace(
                                                                  '"~', '"')
            df[df.columns[c]] = df[df.columns[c]].str.strip('~')
            df[df.columns[c]] = df[df.columns[c]].str.strip(' ')
        if cp_out == '':
            cp_out = cp_in
        dbfadapter.save_dbf(df, encoding=cp_out)

    def save_dbf(df, encoding):
        if encoding == 'utf8' or encoding == '':
            encoding = dbfadapter.combo_codepages_list(
                f'The encoding "{encoding}" is wrong for WRITING dbf. Enter the correct:'
            )

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
            f'Writing {reccount} records into "{os.path.basename(dbfa.finalfile)}" with encoding {encoding}'
        )
        try:
            if os.path.isfile(dbfa.finalfile):
                os.remove(dbfa.finalfile)
            dbfTable = dbf.Table(filename=dbfa.finalfile,
                                 field_specs=newDbfSpecs,
                                 default_data_types=dict(C=dbf.Char),
                                 field_data_types=dict(C=dbf.Char),
                                 codepage=encoding,
                                 dbf_type='vfp')
            dbfTable.open(dbf.READ_WRITE)
        except Exception as error:
            print(f'Break work! Error: {error}')
            sys.exit(1)

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
                    os.remove(dbfa.finalfile)
                    dbfadapter.Messages(
                        f'Break work! Try again with a different encoding of the dbf record\nEncoding {encoding} error: {error}',
                        option=1)
                    return
        sys.stdout.write('\x1b[1A')
        sys.stdout.write('\x1b[2K')
        print('Finished')

    def messages(txtmessage):
        message = Tk()
        message.wm_withdraw()
        print(txtmessage)
        messagebox.showinfo(title="Warning!", message=txtmessage)

    def combo_codepages_list(message):
        psg.theme('LightGray2')
        layout = [
            [
                psg.Text(message,
                         size=(30, 3),
                         font=('Consolas', 10),
                         justification='left')
            ],
            [
                psg.Combo(
                    ['cp1250', 'cp1251', 'cp1252', 'cp850', 'cp852', 'cp866'],
                    key='board',
                    size=(20, 1))
            ], [psg.Button('  OK  ', font=('Consolas', 10))]
        ]
        win = psg.Window('Extract2dbf', layout)
        e, v = win.read()
        win.close()
        if v['board'] == '':
            dbfadapter.Messages(f'Break work! The encoding was not selected.')
            sys.exit(1)
        return v['board']


if __name__ == "__main__":

    def sources():
        sourcefiles = []
        try:
            arg1 = sys.argv[1].replace('\"', '')
            if os.path.isdir(arg1):
                os.chdir(arg1)
                sourcefiles = os.listdir(arg1)
            else:
                sourcefiles.append(sys.argv[1])
        except:
            tk = Tk()
            tk.withdraw()
            sourcefiles = filedialog.askopenfilenames(
                title='Select excel file(s)...',
                filetypes=[(
                    "Supported files",
                    r"*.xls  *.xlsx  *.xlsb  *.xlsm  *.ods  *.odt  *.odf  *.csv"
                ), ("All files", "*.*")])
        return sourcefiles

    cp_in = ''
    cp_out = ''
    sheet = ''
    for a in sys.argv:
        if '-cpin=' in a:
            cp_in = a.replace('-cpin=', '')
        if '-cpout=' in a:
            cp_out = a.replace('-cpout=', '')
        if '-sheet=' in a:
            sheet = a.replace('-sheet=', '')

    if cp_in == '':
        cp_in = default_read_encoding
    if cp_out == '':
        cp_out = default_write_encoding
    if sheet == '':
        sheet = None

    for file in sources():
        dbfa = dbfadapter(file)
        if os.path.isfile(file):
            xlsodsExt = ('.XLS', '.XLSX', '.XLSB', '.XLSM', '.ODS', '.ODT',
                         '.ODF')
            csvExt = ('.CSV')
            fileExt = os.path.splitext(file)[1].upper()
            if fileExt in xlsodsExt:
                data = dbfadapter.read_from_excel(sourcefile=file,
                                                  cp_out=cp_out,
                                                  sheet=sheet)
            if fileExt in csvExt:
                data = dbfadapter.read_from_csv(sourcefile=file,
                                                cp_in=cp_in,
                                                cp_out=cp_out)

    time.sleep(2)

###############################
# ('ascii', "plain ol' ascii"),
# ('cp437', 'U.S. MS-DOS'),
# ('cp850', 'International MS-DOS'),
# ('cp1252', 'Windows ANSI'),
# ('mac_roman', 'Standard Macintosh'),
# ('cp865', 'Danish OEM'),
# ('cp437', 'Dutch OEM'),
# ('cp850', 'Dutch OEM (secondary)'),
# ('cp437', 'Finnish OEM'),
# ('cp437', 'French OEM'),
# ('cp850', 'French OEM (secondary)'),
# ('cp437', 'German OEM'),
# ('cp850', 'German OEM (secondary)'),
# ('cp437', 'Italian OEM'),
# ('cp850', 'Italian OEM (secondary)'),
# ('cp932', 'Japanese Shift-JIS'),
# ('cp850', 'Spanish OEM (secondary)'),
# ('cp437', 'Swedish OEM'),
# ('cp850', 'Swedish OEM (secondary)'),
# ('cp865', 'Norwegian OEM'),
# ('cp437', 'Spanish OEM'),
# ('cp437', 'English OEM (Britain)'),
# ('cp850', 'English OEM (Britain) (secondary)'),
# ('cp437', 'English OEM (U.S.)'),
# ('cp863', 'French OEM (Canada)'),
# ('cp850', 'French OEM (secondary)'),
# ('cp852', 'Czech OEM'),
# ('cp852', 'Hungarian OEM'),
# ('cp852', 'Polish OEM'),
# ('cp860', 'Portugese OEM'),
# ('cp850', 'Potugese OEM (secondary)'),
# ('cp866', 'Russian OEM'),
# ('cp850', 'English OEM (U.S.) (secondary)'),
# ('cp852', 'Romanian OEM'),
# ('cp936', 'Chinese GBK (PRC)'),
# ('cp949', 'Korean (ANSI/OEM)'),
# ('cp950', 'Chinese Big 5 (Taiwan)'),
# ('cp874', 'Thai (ANSI/OEM)'),
# ('cp1252', 'ANSI'),
# ('cp1252', 'Western European ANSI'),
# ('cp1252', 'Spanish ANSI'),
# ('cp852', 'Eastern European MS-DOS'),
# ('cp866', 'Russian MS-DOS'),
# ('cp865', 'Nordic MS-DOS'),
# ('cp861', 'Icelandic MS-DOS'),
# (None, 'Kamenicky (Czech) MS-DOS'),
# (None, 'Mazovia (Polish) MS-DOS'),
# ('cp737', 'Greek MS-DOS (437G)'),
# ('cp857', 'Turkish MS-DOS'),
# ('cp950', 'Traditional Chinese (Hong Kong SAR, Taiwan) Windows'),
# ('cp949', 'Korean Windows'),
# ('cp936', 'Chinese Simplified (PRC, Singapore) Windows'),
# ('cp932', 'Japanese Windows'),
# ('cp874', 'Thai Windows'),
# ('cp1255', 'Hebrew Windows'),
# ('cp1256', 'Arabic Windows'),
# ('cp852', 'Slovenian OEM'),
# ('cp1250', 'Eastern European Windows'),
# ('cp1251', 'Russian Windows'),
# ('cp1254', 'Turkish Windows'),
# ('cp1253', 'Greek Windows'),
# ('mac_cyrillic', 'Russian Macintosh'),
# ('mac_latin2', 'Macintosh EE'),
# ('mac_greek', 'Greek Macintosh'),
# ('utf8', '8-bit unicode'),
