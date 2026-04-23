' Original source extracted from SSIS Script Task: "Database Access Configuration"
' Package: ADDS-MIPS-TC.dtsx (LNI ADDS estate, sanitized sample)
' Language: VisualBasic
' Entry point: ScriptMain.Main()
'
' Captured by ssis_adf_agent's parser as the contents of <ProjectItem
' Name="ScriptMain.vb"> inside the SSIS 2017+ inline VSTA layout (see fix H3).

#Region "Imports"
Imports System
Imports System.Data
Imports System.Math
Imports Microsoft.SqlServer.Dts.Runtime
#End Region

#Region "ScriptResults declaration"
Enum ScriptResults
    Success = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success
    Failure = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure
End Enum
#End Region

<Microsoft.SqlServer.Dts.Tasks.ScriptTask.SSISScriptTaskEntryPointAttribute()> _
<System.CLSCompliantAttribute(False)> _
Partial Public Class ScriptMain
    Inherits Microsoft.SqlServer.Dts.Tasks.ScriptTask.VSTARTScriptObjectModelBase

    ' Set TRUE to display variables and properties from each method call
    Dim DebugOn As Boolean = False

    Public Sub Main()

        Dts.Variables("User::package_run_time").Value = System.DateTime.Now.ToString

        Dim mEnvironment As String = GetEnvironment()
        Dts.Events.FireInformation(0, "", mEnvironment, "", 0, True)

        Dim mDBServerName As String = GetDBServerName()
        Dts.Events.FireInformation(0, "", mDBServerName, "", 0, True)

        Dim mPW As String = GetPW(mEnvironment)
        Dts.Events.FireInformation(0, "PW", mPW, "", 0, True)

        SetSourceConnection(mDBServerName, mPW)

        Dts.TaskResult = ScriptResults.Success
    End Sub

    Private Function GetEnvironment() As String
        Dim Result As String = Dts.Variables("Environment").Value.ToString
        If Result = String.Empty Then
            Dts.Events.FireError(1, "Environment Configuation: ", "VALUE FOR ENVIRONMENT IS NULL", "", 0)
        End If

        If Result = "DEV" Or Result = "LOCAL" Then
            'DebugOn = True
        Else
            DebugOn = False
        End If

        If DebugOn Then MsgBox("Result from function GetEnvironment: " & Result)

        Return Result
    End Function

    Private Function GetDBServerName() As String
        Dim Result As String = Dts.Variables("DatabaseServer").Value.ToString
        If Result = String.Empty Then
            Dts.Events.FireError(1, "DB Server Configuation: ", "VALUE FOR SERVERNAME IS NULL", "", 0)
        End If

        If DebugOn Then MsgBox("Database Server Name: " & Result)

        Return Result
    End Function

    Private Function GetPW(ByVal pEnvironment As String) As String
        Dim result As String = String.Empty
        Dim tempPW As String = String.Empty
        Dim EncryptKey As String = "{6AB5ED47-38D9-4413-8E1E-A73E0BFE551A}"

        'Select Case pEnvironment
        'Case "DEV", "TEST"
        'tempPW = Dts.Variables("PW_WADS").Value.ToString
        'Case "PREPROD", "PROD"
        tempPW = Dts.Variables("PW_LNI").Value.ToString
        ' End Select

        If DebugOn Then MsgBox("Database PW: " & tempPW)
        Return tempPW
    End Function

    Private Sub SetSourceConnection(ByVal pDBServerName As String, ByVal pPW As String)

        Dim conns As Connections = Dts.Connections
        Dim cm As ConnectionManager = conns("Database_Source_Connection_Manager")
        Dim strDatabase As String = Dts.Variables("User::Database").Value.ToString
        Dim strDBUserID As String = Dts.Variables("User::DBUserID").Value.ToString

        cm.Properties("UserName").SetValue(cm, strDBUserID)
        cm.Properties("Password").SetValue(cm, pPW)
        cm.Properties("ServerName").SetValue(cm, pDBServerName)
        cm.Properties("InitialCatalog").SetValue(cm, strDatabase)

        ' Display values during debugging- bypass SSIS debugging limitations
        If DebugOn Then MsgBox("The Database Credentials used are: " & strDBUserID)

        If DebugOn Then
            MsgBox("SOURCE CONNECTIONS: " & cm.Name.ToString & vbCrLf &
            "Server: " & cm.Properties("ServerName") _
            .GetValue(cm).ToString & vbCrLf &
            "Database: " & cm.Properties("InitialCatalog") _
            .GetValue(cm).ToString & vbCrLf &
            "User: " & cm.Properties("UserName") _
            .GetValue(cm).ToString & vbCrLf &
            "Password: " & pPW)
        End If

    End Sub
End Class
