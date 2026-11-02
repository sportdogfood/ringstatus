Set shell = CreateObject("WScript.Shell")
Set files = CreateObject("Scripting.FileSystemObject")
folder = files.GetParentFolderName(WScript.ScriptFullName)
command = "node --env-file=""" & folder & "\runtime\collector.env"" """ & folder & "\cleanup-run.js"""
result = shell.Run(command, 0, True)
WScript.Quit result
