objShellR = Createobject("WScript.Shell")
cmdstr = 'py "n:\!projekty\dbfadapter" "n:\!projekty\dbfadapter\test\test.xlsx" -cpin=cp1252 -cpout=cp1252 -sheet=Sheet1' 

If objShellR.Run(cmdstr, 1, .T.) = 1
	return
endif
