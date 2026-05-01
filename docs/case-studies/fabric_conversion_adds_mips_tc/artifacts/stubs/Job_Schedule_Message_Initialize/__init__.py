"""
Azure Function stub — auto-generated from SSIS Script Task: Job Schedule Message Initialize
Original language: VisualBasic
Entry point: Main

TODO: Implement the business logic below.  The function receives the SSIS
      variables listed under Args as JSON body fields and returns the
      read-write variables in the JSON response.

Args:
        User::csv_file_template: pipeline variable (read-only)
        User::Environment: pipeline variable (read-only)
        User::mail_msg: pipeline variable (read-only)
        User::Package: pipeline variable (read-only)
        User::dest_wrk_path: pipeline variable (read-write)
        User::FileServer: pipeline variable (read-write)
        User::FileServerShare: pipeline variable (read-write)
        User::ReNameFile: pipeline variable (read-write)
        User::SetAttribute: pipeline variable (read-write)
        User::src_wrk_path: pipeline variable (read-write)
"""
import logging
import json
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Executing Job_Schedule_Message_Initialize")

    try:
        body = req.get_json()
    except ValueError:
        body = {}

    csv_file_template = body.get("User::csv_file_template")
    environment = body.get("User::Environment")
    mail_msg = body.get("User::mail_msg")
    package = body.get("User::Package")
    dest_wrk_path = body.get("User::dest_wrk_path")
    fileserver = body.get("User::FileServer")
    fileservershare = body.get("User::FileServerShare")
    renamefile = body.get("User::ReNameFile")
    setattribute = body.get("User::SetAttribute")
    src_wrk_path = body.get("User::src_wrk_path")

    # ---- Original VisualBasic source ----
    # ' --- ScriptMain.vb ---
    # #Region "Imports"
    # Imports System
    # Imports System.Data
    # Imports System.IO
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
    # <Microsoft.SqlServer.Dts.Tasks.ScriptTask.SSISScriptTaskEntryPointAttribute()> _
    # <System.CLSCompliantAttribute(False)> _
    # Partial Public Class ScriptMain
    #     Inherits Microsoft.SqlServer.Dts.Tasks.ScriptTask.VSTARTScriptObjectModelBase
    # 
    #     ' Set TRUE to display variables and properties from each method call
    #     Dim DebugOn As Boolean = False
    #     Public Sub Main()
    #         Dim _sEnviron As String = GetEnvironment()
    #         SetPackageVariables(_sEnviron)
    # 
    #         ' Create message for final email
    #         Dim sMsg As String
    #         sMsg = vbCrLf
    #         sMsg += "AFFRS Test: " + System.DateTime.Now.ToString + vbCrLf + vbCrLf
    #         sMsg += Dts.Variables("User::mail_msg").Value.ToString
    # 
    #         If DebugOn Then MsgBox("Message for final email: " & sMsg)
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
    #             DebugOn = True
    #         Else
    #             DebugOn = False
    #         End If
    # 
    #         If DebugOn Then MsgBox("Result from function GetEnvironment: " & Result)
    # 
    #         Return Result
    #     End Function
    # 
    #     Private Function SetPackageVariables(env As String) As String
    # 
    #         'Get Read-only variables
    #         Dim server As String = Dts.Variables("FileServer").Value.ToString
    #         Dim templateName As String = Dts.Variables("csv_file_template").Value.ToString
    #         Dim packageName As String = Dts.Variables("Package").Value.ToString
    # 
    #         Dts.Events.FireInformation(0, "User::FileServer", server, "", 0, True)
    #         Dts.Events.FireInformation(0, "User::csv_file_template", templateName, "", 0, True)
    #         Dts.Events.FireInformation(0, "User::Package", packageName, "", 0, True)
    # 
    #         '------------------------------------------------------------------------------------------------
    #         'Set FileShare Path
    #         Dim FShare As String
    #         If env = "PROD" Then
    #             'update for PROD File Server Share 
    #             Dts.Variables("FileServerShare").Value = "DW_AFRS\ADDS"
    #             'Dts.Variables("FileServerShare").Value = "DW_AFRS_INT\ADDS"
    #         Else
    #             'Update for Test File Server Share
    #             Dts.Variables("FileServerShare").Value = "DW_AFRS_INT\ADDS"
    #         End If
    # 
    #         FShare = Dts.Variables("FileServerShare").Value.ToString
    #         If DebugOn Then MsgBox("Result File Server Share Path: " & FShare)
    #         Dts.Events.FireInformation(0, "User::FileServerShare", FShare, "", 0, True)
    # 
    #         '------------------------------------------------------------------------------------------------
    #         'Set Source Path
    #         Dts.Variables("src_wrk_path").Value = "\\" + server + "\" + FShare + "\Templates\" + templateName
    # 
    #         Dim src_wrk_path As String = Dts.Variables("src_wrk_path").Value.ToString
    #         If DebugOn Then MsgBox("Template Source: " & src_wrk_path)
    #         Dts.Events.FireInformation(0, "User::src_wrk_path", src_wrk_path, "", 0, True)
    # 
    #         '------------------------------------------------------------------------------------------------
    #         'Set Target Path
    # 
    #         'Dts.Variables("dest_wrk_path").Value = "\\" + server + "\" + FShare + "\Test\"
    #         Dts.Variables("dest_wrk_path").Value = "\\" + server + "\" + FShare + "\MIPS\"
    #         Dim tgt_wrk_path As String = Dts.Variables("dest_wrk_path").Value.ToString
    # 
    #         If DebugOn Then MsgBox("Target Share: " & tgt_wrk_path)
    #         Dts.Events.FireInformation(0, "User::dest_wrk_path", tgt_wrk_path, "", 0, True)
    # 
    #         '------------------------------------------------------------------------------------------------
    #         'Set Attribute Variable
    #         Dts.Variables("SetAttribute").Value = tgt_wrk_path + templateName
    # 
    #         Dim setAttribute As String = Dts.Variables("SetAttribute").Value.ToString
    #         If DebugOn Then MsgBox("New Set Attribute path: " & setAttribute)
    #         Dts.Events.FireInformation(0, "User::SetAttribute", setAttribute, "", 0, True)
    # 
    #         '------------------------------------------------------------------------------------------------
    #         'Set File Rename Variable
    #         'Dim newFileName As String = Dts.Variables("ReNameFile").Value.ToString
    #         Dim newFileName As String = "transaction_control"
    # 
    #         Dts.Variables("ReNameFile").Value = tgt_wrk_path + newFileName + ".txt"
    # 
    # 
    #         If DebugOn Then MsgBox("New File Name: " & newFileName)
    #         Dts.Events.FireInformation(0, "User::ReNameFile", newFileName, "", 0, True)
    # 
    #         '------------------------------------------------------------------------------------------------
    #         Return "Completed"
    # 
    #     End Function
    # 
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
    # <Assembly: AssemblyTitle("ST_edcdfe2359964d70ad4ccf1f0cd378e2")> 
    # <Assembly: AssemblyDescription("")> 
    # <Assembly: AssemblyCompany("")> 
    # <Assembly: AssemblyProduct("ST_edcdfe2359964d70ad4ccf1f0cd378e2")> 
    # <Assembly: AssemblyCopyright("Copyright @  2018")> 
    # <Assembly: AssemblyTrademark("")> 
    # <Assembly: CLSCompliant(True)> 
    # 
    # <Assembly: ComVisible(False)> 
    # 
    # 'The following GUID is for the ID of the typelib if this project is exposed to COM
    # <Assembly: Guid("9dc40811-2f34-473b-b20d-a9eb1b22ebdb")> 
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
        "Script Task 'Job Schedule Message Initialize' has not been implemented yet. "
        "See the original VisualBasic code above."
    )

    return func.HttpResponse(
        json.dumps({"User::dest_wrk_path": dest_wrk_path, "User::FileServer": fileserver, "User::FileServerShare": fileservershare, "User::ReNameFile": renamefile, "User::SetAttribute": setattribute, "User::src_wrk_path": src_wrk_path}),
        mimetype="application/json",
        status_code=200,
    )
