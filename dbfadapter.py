import sys
import os
import dbf
import pandas as pd
from tkinter import messagebox
from tkinter import *
from tqdm import tqdm
import magic
import PySimpleGUI as psg


class convert:

    def __init__(self, sourcefile, cp_in, cp_out, sheet, ext):
        self.sourcefile = sourcefile
        self.ext = ext
        self.cp_in = cp_in
        self.cp_out = cp_out
        self.sheet = sheet
        self.sep = self.detect_separator_in_csv()
        self.finalfile = os.path.splitext(self.sourcefile)[0] + '.dbf'
        self.filename = os.path.basename(sourcefile)
        self.detect_encoding_in_csv()

    def detect_separator_in_csv(self):
        if self.ext == 'CSV':
            line = open(self.sourcefile, "rb")
            head = line.readline()
            line.close()
            separators = '|;\t,'
            try:
                for separator in separators:
                    if separator in head.decode():
                        break
            except Exception as error:
                print(f'Break work! No separtor found. Error: "{error}"')
            print(f'Detected csv separator: "{separator}"')
            return separator

    def detect_encoding_in_csv(self):
        if self.cp_in == '' and self.ext == 'CSV':
            cp = magic.Magic(mime_encoding=True,
                             keep_going=False).from_file(self.sourcefile)
            cp = cp.replace('windows-', 'cp').replace(
                'iso-8859-1',
                'cp1252').replace('iso-8859-5', 'cp1251').replace(
                    'iso-8859-7',
                    'cp1253').replace('iso-8859-8',
                                      'cp1255').replace('utf-8', 'utf8')
            if cp[:7] == 'unknown':
                cp = self.codepages_list(
                    f'The encoding "{cp}" is wrong for READING csv. Enter the correct:'
                )
            print(f'Detected csv encoding: "{cp}"')
            self.cp_in = cp
            if self.cp_out == '':
                self.cp_out = self.cp_in

    def write_from_excel(self):
        print(f'Reading data from "{self.filename}"')
        try:
            excelFile = pd.read_excel(self.sourcefile,
                                      dtype="str",
                                      sheet_name=self.sheet)
        except Exception as error:
            print(f'Break work! Error: {error}')
            sys.exit(1)
        if 'DataFrame' in str(type(excelFile)):
            excelFile = {self.sheet: excelFile}
        sheetCnt = 0
        for lsheet in excelFile:
            self.finalfile = os.path.splitext(
                self.sourcefile)[0] + '_' + lsheet + '.dbf'
            df = pd.DataFrame(excelFile[lsheet])
            if len(df) > 0:
                self.save_dbf(df)
                sheetCnt += 1
        if sheetCnt > 1:
            self.messages("The file contains more than one sheet...")

    def write_from_csv(self):

        print(
            f'Reading data from "{self.filename}" with encoding {self.cp_in}')

        try:
            df = pd.read_csv(
                self.sourcefile,
                dtype="str",
                engine='python',
                delimiter=self.sep,
                # sep=f'"*[{sep}]"*',
                encoding=self.cp_in,
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

        self.save_dbf(df)

    def save_dbf(self, df):
        if self.cp_out == 'utf8' or self.cp_out == '':
            self.cp_out = self.codepages_list(
                f'The encoding "{self.cp_out}" is wrong for WRITING dbf. Enter the correct:'
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
            f'Writing {reccount} records into "{self.finalfile}" with encoding {self.cp_out}'
        )
        try:
            if os.path.isfile(self.finalfile):
                os.remove(self.finalfile)
            dbfTable = dbf.Table(filename=self.finalfile,
                                 field_specs=newDbfSpecs,
                                 default_data_types=dict(C=dbf.Char),
                                 field_data_types=dict(C=dbf.Char),
                                 codepage=self.cp_out,
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
                    os.remove(self.finalfile)
                    self.Messages(
                        f'Break work! Try again with a different encoding of the dbf record\nEncoding {self.cp_out} error: {error}',
                        option=1)
                    return
        sys.stdout.write('\x1b[1A')
        sys.stdout.write('\x1b[2K')
        print('Finished')

    def messages(self, txtmessage):
        message = Tk()
        message.wm_withdraw()
        print(txtmessage)
        messagebox.showinfo(title="Warning!", message=txtmessage)

    def codepages_list(self, message):
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
            self.Messages(f'Break work! The encoding was not selected.')
            sys.exit(1)
        return v['board']


def parse_args():
    path = ''
    try:
        path = sys.argv[1]
    except:
        pass
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
    if sheet == '':
        sheet = None

    return path, cp_in, cp_out, sheet


def convert_file(sourcefile, cp_in, cp_out, sheet=None):

    dbfconv = convert(sourcefile, cp_in, cp_out, sheet,
                      os.path.splitext(sourcefile)[1].replace('.', '').upper())
    if dbfconv.ext == 'CSV':
        dbfconv.write_from_csv()
    else:
        dbfconv.write_from_excel()


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
