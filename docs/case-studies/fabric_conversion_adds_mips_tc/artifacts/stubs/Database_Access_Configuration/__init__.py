"""
Azure Function stub — auto-generated from SSIS Script Task: Database Access Configuration
Original language: VisualBasic
Entry point: Main

TODO: Implement the business logic below.  The function receives the SSIS
      variables listed under Args as JSON body fields and returns the
      read-write variables in the JSON response.

Args:
        User::Database: pipeline variable (read-only)
        User::DatabaseServer: pipeline variable (read-only)
        User::DBUserID: pipeline variable (read-only)
        User::Environment: pipeline variable (read-only)
        User::PW_LNI: pipeline variable (read-only)
        User::PW_WADS: pipeline variable (read-only)
        User::package_run_time: pipeline variable (read-write)
"""
import logging
import json
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Executing Database_Access_Configuration")

    try:
        body = req.get_json()
    except ValueError:
        body = {}

    database = body.get("User::Database")
    databaseserver = body.get("User::DatabaseServer")
    dbuserid = body.get("User::DBUserID")
    environment = body.get("User::Environment")
    pw_lni = body.get("User::PW_LNI")
    pw_wads = body.get("User::PW_WADS")
    package_run_time = body.get("User::package_run_time")

    # ---- Original VisualBasic source ----
    # ' --- ScriptMain.vb ---
    # #Region "Imports"
    # Imports System
    # Imports System.Data
    # Imports System.Math
    # Imports Microsoft.SqlServer.Dts.Runtime
    # #End Region
    # 
    # #Region "ScriptResults declaration"
    # 'This enum provides a convenient shorthand within the scope of this class for setting the
    # 'result of the script.
    # 
    # 'This code was generated automatically.
    # Enum ScriptResults
    #     Success = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success
    #     Failure = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure
    # End Enum
    # 
    # #End Region
    # 
    # 'ScriptMain is the entry point class of the script.  Do not change the name, attributes,
    # 'or parent of this class.
    # <Microsoft.SqlServer.Dts.Tasks.ScriptTask.SSISScriptTaskEntryPointAttribute()> _
    # <System.CLSCompliantAttribute(False)> _
    # Partial Public Class ScriptMain
    #     Inherits Microsoft.SqlServer.Dts.Tasks.ScriptTask.VSTARTScriptObjectModelBase
    # 
    #     ' Set TRUE to display variables and properties from each method call
    #     Dim DebugOn As Boolean = False
    # 
    #     Public Sub Main()
    # 
    #         Dts.Variables("User::package_run_time").Value = System.DateTime.Now.ToString
    # 
    #         Dim mEnvironment As String = GetEnvironment()
    #         Dts.Events.FireInformation(0, "", mEnvironment, "", 0, True)
    # 
    #         Dim mDBServerName As String = GetDBServerName()
    #         Dts.Events.FireInformation(0, "", mDBServerName, "", 0, True)
    # 
    #         Dim mPW As String = GetPW(mEnvironment)
    #         Dts.Events.FireInformation(0, "PW", mPW, "", 0, True)
    # 
    #         SetSourceConnection(mDBServerName, mPW)
    # 
    #         Dts.TaskResult = ScriptResults.Success
    #     End Sub
    # 
    #     Private Function GetEnvironment() As String
    #         Dim Result As String = Dts.Variables("Environment").Value.ToString
    #         If Result = String.Empty Then
    #             Dts.Events.FireError(1, "Environment Configuation: ", "VALUE FOR ENVIRONMENT IS NULL", "", 0)
    #         End If
    # 
    #         If Result = "DEV" Or Result = "LOCAL" Then
    #             'DebugOn = True
    #         Else
    #             DebugOn = False
    #         End If
    # 
    #         If DebugOn Then MsgBox("Result from function GetEnvironment: " & Result)
    # 
    #         Return Result
    #     End Function
    # 
    #     Private Function GetDBServerName() As String
    #         Dim Result As String = Dts.Variables("DatabaseServer").Value.ToString
    #         If Result = String.Empty Then
    #             Dts.Events.FireError(1, "DB Server Configuation: ", "VALUE FOR SERVERNAME IS NULL", "", 0)
    #         End If
    # 
    #         If DebugOn Then MsgBox("Database Server Name: " & Result)
    # 
    #         Return Result
    #     End Function
    # 
    #     Private Function GetPW(ByVal pEnvironment As String) As String
    #         Dim result As String = String.Empty
    #         Dim tempPW As String = String.Empty
    #         Dim EncryptKey As String = "{6AB5ED47-38D9-4413-8E1E-A73E0BFE551A}"
    # 
    #         'Select Case pEnvironment
    #         'Case "DEV", "TEST"
    #         'tempPW = Dts.Variables("PW_WADS").Value.ToString
    #         'Case "PREPROD", "PROD"
    #         tempPW = Dts.Variables("PW_LNI").Value.ToString
    #         ' End Select
    # 
    #         If DebugOn Then MsgBox("Database PW: " & tempPW)
    #         Return tempPW
    #     End Function
    # 
    #     Private Sub SetSourceConnection(ByVal pDBServerName As String, ByVal pPW As String)
    # 
    #         Dim conns As Connections = Dts.Connections
    #         Dim cm As ConnectionManager = conns("Database_Source_Connection_Manager")
    #         Dim strDatabase As String = Dts.Variables("User::Database").Value.ToString
    #         Dim strDBUserID As String = Dts.Variables("User::DBUserID").Value.ToString
    # 
    #         cm.Properties("UserName").SetValue(cm, strDBUserID)
    #         cm.Properties("Password").SetValue(cm, pPW)
    #         cm.Properties("ServerName").SetValue(cm, pDBServerName)
    #         cm.Properties("InitialCatalog").SetValue(cm, strDatabase)
    # 
    #         ' Display values during debugging- bypass SSIS debugging limitations
    #         If DebugOn Then MsgBox("The Database Credentials used are: " & strDBUserID)
    # 
    #         If DebugOn Then
    #             MsgBox("SOURCE CONNECTIONS: " & cm.Name.ToString & vbCrLf &
    #             "Server: " & cm.Properties("ServerName") _
    #             .GetValue(cm).ToString & vbCrLf &
    #             "Database: " & cm.Properties("InitialCatalog") _
    #             .GetValue(cm).ToString & vbCrLf &
    #             "User: " & cm.Properties("UserName") _
    #             .GetValue(cm).ToString & vbCrLf &
    #             "Password: " & pPW)
    #         End If
    # 
    #     End Sub
    # End Class
    # 
    # ' --- My Project\AssemblyInfo.vb ---
    # Imports System
    # Imports System.Reflection
    # Imports System.Runtime.InteropServices
    # 
    # ' General Information about an assembly is controlled through the following 
    # ' set of attributes. Change these attribute values to modify the information
    # ' associated with an assembly.
    # 
    # ' Review the values of the assembly attributes
    # 
    # <Assembly: AssemblyTitle("ST_8d54f04565e44e10b50fc04d84412b9c")> 
    # <Assembly: AssemblyDescription("")> 
    # <Assembly: AssemblyCompany("")> 
    # <Assembly: AssemblyProduct("ST_8d54f04565e44e10b50fc04d84412b9c")> 
    # <Assembly: AssemblyCopyright("Copyright @  2018")> 
    # <Assembly: AssemblyTrademark("")> 
    # <Assembly: CLSCompliant(True)> 
    # 
    # <Assembly: ComVisible(False)> 
    # 
    # 'The following GUID is for the ID of the typelib if this project is exposed to COM
    # <Assembly: Guid("98bcde88-a6f0-4866-bc37-8c26280b7d9f")> 
    # 
    # ' Version information for an assembly consists of the following four values:
    # '
    # '      Major Version
    # '      Minor Version 
    # '      Build Number
    # '      Revision
    # '
    # ' You can specify all the values or you can default the Build and Revision Numbers 
    # ' by using the '*' as shown below:
    # ' <Assembly: AssemblyVersion("1.0.*")>
    # 
    # <Assembly: AssemblyVersion("1.0.0.0")> 
    # <Assembly: AssemblyFileVersion("1.0.0.0")>
    # 
    # ' --- My Project\Settings.Designer.vb ---
    # '------------------------------------------------------------------------------
    # ' <autogenerated>
    # '     This code was generated by a tool.
    # '
    # '     Changes to this file may cause incorrect behavior and will be lost if
    # '     the code is regenerated.
    # ' </autogenerated>
    # '------------------------------------------------------------------------------
    # 
    # Option Strict Off
    # Option Explicit On
    # 
    # 
    # 
    # Partial Friend NotInheritable Class MySettings
    #     Inherits System.Configuration.ApplicationSettingsBase
    # 
    #     Private Shared m_Value As MySettings
    # 
    #     Private Shared m_SyncObject As Object = New Object
    # 
    #     <System.Diagnostics.DebuggerNonUserCode()> _
    #     Public Shared ReadOnly Property Value() As MySettings
    #         Get
    #             If (MySettings.m_Value Is Nothing) Then
    #                 System.Threading.Monitor.Enter(MySettings.m_SyncObject)
    #                 If (MySettings.m_Value Is Nothing) Then
    #                     Try
    #                         MySettings.m_Value = New MySettings
    #                     Finally
    #                         System.Threading.Monitor.Exit(MySettings.m_SyncObject)
    #                     End Try
    #                 End If
    #             End If
    #             Return MySettings.m_Value
    #         End Get
    #     End Property
    # End Class
    # 
    # ' --- My Project\Resources.Designer.vb ---
    # '------------------------------------------------------------------------------
    # ' <autogenerated>
    # '     This code was generated by a tool.
    # '
    # '     Changes to this file may cause incorrect behavior and will be lost if
    # '     the code is regenerated.
    # ' </autogenerated>
    # '------------------------------------------------------------------------------
    # 
    # Option Strict Off
    # Option Explicit On
    # 
    # 
    # Namespace My.Resources
    #     
    #     '''<summary>
    #     '''   A strongly-typed resource class, for looking up localized strings, etc.
    #     '''</summary>
    #     'This class was auto-generated by the Strongly Typed Resource Builder
    #     'class via a tool like ResGen or Visual Studio.NET.
    #     'To add or remove a member, edit your .ResX file then rerun ResGen
    #     'with the /str option, or rebuild your VS project.
    #     Class MyResources
    #         
    #         Private Shared _resMgr As System.Resources.ResourceManager
    #         
    #         Private Shared _resCulture As System.Globalization.CultureInfo
    #         
    #         Friend Sub New()
    #             MyBase.New
    #         End Sub
    #         
    #         '''<summary>
    #         '''   Returns the cached ResourceManager instance used by this class.
    #         '''</summary>
    #         <System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)>  _
    #         Public Shared ReadOnly Property ResourceManager() As System.Resources.ResourceManager
    #             Get
    #                 If (_resMgr Is Nothing) Then
    #                     Dim temp As System.Resources.ResourceManager = New System.Resources.ResourceManager("My.Resources.MyResources", GetType(MyResources).Assembly)
    #                     _resMgr = temp
    #                 End If
    #                 Return _resMgr
    #             End Get
    #         End Property
    #         
    #         '''<summary>
    #         '''   Overrides the current thread's CurrentUICulture property for all
    #         '''   resource lookups using this strongly typed resource class.
    #         '''</summary>
    #         <System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Advanced)>  _
    #         Public Shared Property Culture() As System.Globalization.CultureInfo
    #             Get
    #                 Return _resCulture
    #             End Get
    #             Set
    #                 _resCulture = value
    #             End Set
    #         End Property
    #     End Class
    # End Namespace

    # TODO: implement converted logic here
    raise NotImplementedError(
        "Script Task 'Database Access Configuration' has not been implemented yet. "
        "See the original VisualBasic code above."
    )

    return func.HttpResponse(
        json.dumps({"User::package_run_time": package_run_time}),
        mimetype="application/json",
        status_code=200,
    )
