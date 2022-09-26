import sys
import os
import dbf
import pandas as pd
import magic
from tqdm import tqdm
import constants
import tools


class convert:
    def __init__(self, sourcefile, cp_in, cp_out, sheet, ext):
        self.sourcefile = sourcefile
        self.ext = ext
        self.cp_in = cp_in
        self.cp_out = cp_out
        self.sheet = sheet
        self.filename = os.path.basename(self.sourcefile)

    def detect_separator_in_csv(self):
        try:
            line = open(self.sourcefile, "r")
            head = line.readline()
            line.close()
            separators = "|;\t,"
            for separator in separators:
                if separator in head:
                    print(f'Detected csv separator: "{separator}"')
                    return separator
        except Exception as error:
            print(error)

    def detect_encoding_in_csv(self):
        if self.cp_in == "":
            try:
                cp = magic.Magic(mime_encoding=True, keep_going=False).from_file(
                    self.sourcefile
                )
                if "cannot open" in cp:
                    print("Can't open file for encoding detection.")
                    cp = ""
            except Exception as error:
                print(error)

            if cp not in constants.supportedcsvcp:
                cp = tools.codepages_list(
                    f'The encoding "{cp}" is wrong for READING csv. Enter the correct:',
                    constants.supportedcsvcp,
                )

            print(f'Detected csv encoding: "{cp}"')

            self.cp_in = cp
            if self.cp_out == "":
                self.cp_out = self.cp_in.replace("utf-8", "utf8")

    def write_from_excel(self):
        print(f'Reading data from "{self.filename}"')

        try:
            dfxl = pd.read_excel(
                self.sourcefile,
                dtype="str",
                sheet_name=self.sheet,
            )
        except Exception as error:
            print(error)

        if "DataFrame" in str(type(dfxl)):
            dfxl = {self.sheet: dfxl}
        sheetCnt = 0

        for lsheet in dfxl:
            df = pd.DataFrame(dfxl[lsheet])
            if len(df) > 0:
                sheetCnt += self.save_dbf(
                    df, os.path.splitext(self.sourcefile)[0] + "_" + lsheet + ".dbf"
                )

        if sheetCnt > 1:
            tools.messages("The file contains more than one sheet...")

    def write_from_csv(self):
        self.detect_encoding_in_csv()
        sep = self.detect_separator_in_csv()

        if sep == "":
            print("No separator found!")

        print(f'Reading data from "{self.filename}" with encoding {self.cp_in}')

        try:
            df = pd.read_csv(
                self.sourcefile,
                engine="c",
                dtype="str",
                sep=sep,
                encoding=self.cp_in,
                quoting=3,
                keep_default_na=False,
            )
        except Exception as error:
            print(error)

        for c in range(len(df.columns)):
            df[df.columns[c]] = df[df.columns[c]].str.replace('""', '"~"')
            df[df.columns[c]] = df[df.columns[c]].str.strip('"')
            df[df.columns[c]] = df[df.columns[c]].str.replace('~"', '"')
            df[df.columns[c]] = df[df.columns[c]].str.replace('"~', '"')
            df[df.columns[c]] = df[df.columns[c]].str.strip("~")
            df[df.columns[c]] = df[df.columns[c]].str.strip(" ")

        self.save_dbf(df, os.path.splitext(self.sourcefile)[0] + ".dbf")

    def save_dbf(self, df, finalfile):
        if self.cp_out not in constants.supporteddbfcp or self.cp_out == "":
            self.cp_out = tools.codepages_list(
                f'The encoding "{self.cp_out}" is wrong for WRITING dbf. Enter the correct:',
                constants.supporteddbfcp,
            )

        df.fillna("", inplace=True)
        reccount = len(df)
        if len(df) < 1:
            return

        newDbfSpecs = ""
        fldList = []
        badchar = constants.badchars
        chartorepl = constants.charstorepl
        for c in range(len(df.columns)):
            col = df.columns[c]
            for char in badchar:
                col = col.replace(char, "").replace("ß", "ss")
            if col[1] in "01234567890":
                col = "_" + col
            for s, r in chartorepl.items():
                col = col.replace(s, r)

            col = col[0:10]
            if col not in fldList:
                fldList.append(col)
            else:
                i = 1
                while col in fldList:
                    i += 1
                    col = col[0:8] + str(i).rjust(2, "0")
                fldList.append(col)

            maxFldLen = max(df.iloc[:, c].astype(str).apply(len))
            fldInfo = f"{col} C( {str(maxFldLen + 10)} );"
            if maxFldLen > 265:
                fldInfo = f"{col} M;"

            newDbfSpecs += fldInfo
            # replace char \x81
            df[df.columns[c]] = df[df.columns[c]].str.replace(chr(129), chr(252))

        print(
            f'Writing {reccount} records into "{os.path.basename(finalfile)}" with encoding {self.cp_out}'
        )

        try:
            if os.path.isfile(finalfile):
                os.remove(finalfile)

            dbfTable = dbf.Table(
                filename=finalfile,
                field_specs=newDbfSpecs,
                default_data_types=dict(C=dbf.Char),
                field_data_types=dict(C=dbf.Char),
                codepage=self.cp_out,
                dbf_type="vfp",
            )

            dbfTable.open(dbf.READ_WRITE)
        except Exception as error:
            print(error)

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
                    os.remove(finalfile)
                    self.exceptions(error)

        sys.stdout.write("\x1b[1A")
        sys.stdout.write("\x1b[2K")
        print("Finished")
        return 1


def convert_file(sourcefile, cp_in, cp_out, sheet=None):
    if os.path.splitext(sourcefile)[1].lower() in constants.supportedfiles:
        dbfconv = convert(
            sourcefile,
            cp_in,
            cp_out,
            sheet,
            os.path.splitext(sourcefile)[1].replace(".", "").upper(),
        )
        try:
            if dbfconv.ext == "CSV":
                dbfconv.write_from_csv()
            else:
                dbfconv.write_from_excel()
        except Exception as error:
            print(f"Break work. {error}")
            tools.messages(f"Break work. {error}")
    else:
        print("Wrong file type.")
