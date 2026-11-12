Set shell = CreateObject("WScript.Shell")
Set files = CreateObject("Scripting.FileSystemObject")
folder = files.GetParentFolderName(WScript.ScriptFullName)
command = """C:\Program Files\nodejs\node.exe"" --env-file=""" & folder & "\runtime\collector.env"" """ & folder & "\day1-entry.js"""
If WScript.Arguments.Named.Exists("day") Then command = command & " --day " & WScript.Arguments.Named.Item("day")
If WScript.Arguments.Named.Exists("rerun") Then command = command & " --rerun"
WScript.Quit shell.Run(command, 0, True)
