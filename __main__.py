import os
from tkinter import filedialog
from tkinter import *
import time
import dbfadapter as da
import constants

supportedfiles = constants.supportedfiles

if __name__ == "__main__":

    def sources(path):
        sourcefiles = []
        if path != "":
            if os.path.isdir(path):
                os.chdir(path)
                sourcefiles = os.listdir(path)
            else:
                sourcefiles.append(path)
        else:
            tk = Tk()
            tk.withdraw()
            sourcefiles = filedialog.askopenfilenames(
                title="Select excel file(s)...",
                filetypes=[("Supported files", supportedfiles), ("All files", "*.*")],
            )
        return sourcefiles

    args = da.parse_args()

    for file in sources(args[0]):
        if os.path.splitext(file)[1].lower() in supportedfiles:
            da.convert_file(
                sourcefile=file, cp_in=args[1], cp_out=args[2], sheet=args[3]
            )

    time.sleep(1)
