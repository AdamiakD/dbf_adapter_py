import time
import dbfadapter as da
import tools


if __name__ == "__main__":

    source = tools.detect_sources()
    for file in source[0]:
        da.convert_file(
            sourcefile=file, cp_in=source[1], cp_out=source[2], sheet=source[3]
        )
    time.sleep(1)
