import sys
import os
import PySimpleGUI as psg
from tkinter import filedialog
from tkinter import *
import constants


def messages(txtmessage):
    psg.theme("LightGray2")
    layout = [
        [
            psg.Text(
                txtmessage,
                auto_size_text=True,
                font=("Consolas", 10),
                justification="left",
            )
        ],
        [psg.Button("   OK   ", font=("Consolas", 10))],
    ]
    window = psg.Window("Dbf adapter", layout, element_justification="center")
    window.read()
    window.close()


def combos(txtmessage, supportedcp):
    psg.theme("LightGray2")
    layout = [
        [
            psg.Text(
                txtmessage,
                size=(40, 3),
                font=("Consolas", 10),
                justification="center",
            )
        ],
        [
            psg.Combo(
                supportedcp,
                default_value="cp1252",
                key="board",
                size=(20, 1),
            )
        ],
        [
            psg.Button("  OK  ", font=("Consolas", 10)),
            psg.Button("CANCEL", font=("Consolas", 10)),
        ],
    ]
    win = psg.Window(
        "Dbf adapter",
        layout,
        element_justification="center",
    )
    e, v = win.read()
    win.close()
    if e == "CANCEL" or e is None:
        return
    return v["board"]


def detect_sources():
    path = ""

    try:
        path = sys.argv[1]
    except:
        pass

    cp_in = ""
    cp_out = ""
    sheet = ""
    for a in sys.argv:
        if "-cpin=" in a:
            cp_in = a.replace("-cpin=", "")
        if "-cpout=" in a:
            cp_out = a.replace("-cpout=", "")
        if "-sheet=" in a:
            sheet = a.replace("-sheet=", "")
    if sheet == "":
        sheet = None

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
            filetypes=[
                ("Supported files", constants.supportedfiles),
                ("All files", "*.*"),
            ],
        )
    return sourcefiles, cp_in, cp_out, sheet
