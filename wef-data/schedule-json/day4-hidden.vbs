Set shell = CreateObject("WScript.Shell")
Set files = CreateObject("Scripting.FileSystemObject")
folder = files.GetParentFolderName(WScript.ScriptFullName)
parent = files.GetParentFolderName(folder)
command = """C:\Program Files\nodejs\node.exe"" --env-file=""" & parent & "\runtime\collector.env"" """ & folder & "\day4.mjs"""
WScript.Quit shell.Run(command, 0, True)
