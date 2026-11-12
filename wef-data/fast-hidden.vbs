Set shell = CreateObject("WScript.Shell")
Set files = CreateObject("Scripting.FileSystemObject")
folder = files.GetParentFolderName(WScript.ScriptFullName)
script = "fast-entry.js"
If WScript.Arguments.Named.Exists("linking") Then script = "fast\linker.js"
command = """C:\Program Files\nodejs\node.exe"" --env-file=""" & folder & "\runtime\collector.env"" """ & folder & "\" & script & """"
If WScript.Arguments.Named.Exists("day") Then command = command & " --day " & WScript.Arguments.Named.Item("day")
If WScript.Arguments.Named.Exists("saving") Then command = command & " --saving"
WScript.Quit shell.Run(command, 0, True)
