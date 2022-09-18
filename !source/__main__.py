import sys
import os
from tkinter import filedialog
from tkinter import *
import time
import dbfadapter as da

default_read_encoding = ''
default_write_encoding = ''

if __name__ == "__main__":

    def sources(path):
        sourcefiles = []
        try:
            arg1 = path.replace('\"', '')
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

    args = da.parse_args()

    for file in sources(args[0]):
        da.convert_file(sourcefile=file,
                        cp_in=args[1],
                        cp_out=args[2],
                        sheet=args[3])

    time.sleep(1)