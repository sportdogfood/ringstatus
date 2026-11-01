Set shell = CreateObject("WScript.Shell")
Set files = CreateObject("Scripting.FileSystemObject")
folder = files.GetParentFolderName(WScript.ScriptFullName)
command = "powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File """ & folder & "\run.ps1"""
If WScript.Arguments.Named.Exists("linking") Then command = command & " -Linking"
result = shell.Run(command, 0, True)
WScript.Quit result
