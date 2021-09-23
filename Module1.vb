Imports System.Data.SqlClient
Imports System.IO
Imports System.Threading

Module Module1


    Public strConnOld, strConnNew As String
    Public BatchSize As Integer
    Public dbDT_Stops, tblNewStatus, tblNewXVICond, tblNewStops As DataTable

    Sub Main()
        Dim StartOrigStat, MaxOrigStat, LastOrigStat, LastNewStat As Long
        Dim StopCount, AllCount, RunningCount, KeyVal As Long
        Dim dbDA_Stops As SqlDataAdapter
        Dim dbCmdRStops As SqlCommand
        Dim dbConnOld As SqlConnection
        Dim tmpSql, strReadStopsSQL, strKeyVal As String
        Dim AllDone As Boolean
        Dim ProgRun As New Stopwatch
        Dim TSpan2 As New TimeSpan

        Try
            'Assign Connection Strings.
            If SetConnections() = False Then
                Throw New Exception("Failure to sassign connection strings. SetConnections()")
            End If
            dbConnOld = New SqlConnection(strConnOld)
            dbConnOld.Open()
            'dbConnNew = New SqlConnection(strConnNew)
            'dbConnNew.Open()

            'Get Startup StatusID value from keyboard
            Console.WriteLine("Enter the last read StatusID. Enter 0 if starting over.")
            Console.Write("Enter StatusID value here: ")
            strKeyVal = Console.ReadLine()
            If IsNumeric(strKeyVal) Then
                KeyVal = CInt(strKeyVal)
                If KeyVal >= 0 Then
                    If MsgBox("Entered value is: " & KeyVal.ToString & vbCrLf & "Do you want to proceed?", MsgBoxStyle.YesNo) = vbNo Then
                        End
                    End If
                Else
                    MsgBox("Value must be positive or zero, loser.", MsgBoxStyle.Critical, "Data Entry Validation")
                    End
                End If
            Else
                MsgBox("Value Is Not numeric.", MsgBoxStyle.Critical, "Data Entry Validation")
                End
            End If

            'Start Stopwatch
            ProgRun.Start()

            'Fill the Stops data table with all the original stops
            'Places full results into DataTable dbDT_Stops
            strReadStopsSQL = "SELECT * FROM [Stops] ;"
            dbCmdRStops = New SqlCommand(strReadStopsSQL, dbConnOld) : dbCmdRStops.CommandTimeout = 0
            dbDT_Stops = New DataTable
            dbDA_Stops = New SqlDataAdapter(dbCmdRStops)
            StopCount = dbDA_Stops.Fill(dbDT_Stops)
            If StopCount = 0 Then
                Throw New Exception("Invalid RecordCount from initial dbDT_Stops datatable fill command.")
            End If

            'This value can be set to allow for picking up where it previously ended.
            'Enter the value of the last original StatusID processed. This will be in a message box at the end
            'and printed in the immediate window, if runn in VS.
            'LastOrigStat = 6500000
            LastOrigStat = KeyVal

            'Queries Original Status_RST-XVI to get number or rows.
            tmpSql = "Select COUNT([StatusID]) FROM [Status_RST-XVI] Where " _
                    & "(Stamp >= CONVERT(DATETIME, '2019-04-01 00:00:00', 102)) AND " _
                    & "(StatusID > " & LastOrigStat.ToString & " );" ' ORDER BY StatusID ASC;"

            Dim cmdOrig As New SqlCommand(tmpSql, dbConnOld)
            Dim dtOrig = New DataTable
            Dim daOrig = New SqlDataAdapter(cmdOrig)
            AllCount = cmdOrig.ExecuteScalar()
            If AllCount = 0 Then
                Throw New Exception("Invalid AllCount from initial dbo.Status_RST-XVI table.")
            End If

            tmpSql = "SELECT MIN(StatusID) FROM [Status_RST-XVI] WHERE " _
                & "(Stamp >= CONVERT(DATETIME, '2019-04-01 00:00:00', 102)) AND " _
                & "(StatusID > " & LastOrigStat.ToString & " );" ' ORDER BY StatusID ASC;"
            cmdOrig.CommandText = tmpSql
            StartOrigStat = cmdOrig.ExecuteScalar
            If StartOrigStat = 0 Then
                Throw New Exception("Invalid StartOrigStat from initial dbo.Status_RST-XVI table.")
            End If
            cmdOrig.CommandText = "SELECT MAX(StatusID) FROM [Status_RST-XVI];"
            MaxOrigStat = cmdOrig.ExecuteScalar
            dtOrig.Dispose() : daOrig.Dispose()

            'Structure the three new working DataTables
            'tblNewStatus, tblNewXVICond, tblNewStops
            If Not MakeStatusTable(1) = True Then
                Throw New Exception("Exception making tblNewStatus.")
            End If
            If Not MakeXVICondTable() = True Then
                Throw New Exception("Exception making tblNewXVICond.")
            End If
            If Not MakeStopsTable() = True Then
                Throw New Exception("Exception making tblNewStops.")
            End If

            'This seeds the new DataTable for identity to be appendable to dbo.Machine_Status
            If Not SeedStatusTable() = True Then
                Throw New Exception("Exception Seeding tblNewStatus.")
            End If

            'Informational purposes
            Console.CursorLeft = 0 : Console.CursorTop = 3
            Console.Write("Started at: " & Now.ToString)

            AllDone = False
            BatchSize = 8000
            RunningCount = 0
            LastOrigStat = StartOrigStat - 1 'Increment down by 1 for initial batch
            LastNewStat = 0
            Do
                Console.CursorLeft = 0 : Console.CursorTop = 4
                Console.Write("Completed " & Format(RunningCount, "N0") & " of " & Format(AllCount, "N0"))

                'This passes the starting StatusID field for the Select query in ConvertData
                'and returns the last New StatusID so the Seed setting can be used for the next batch.
                LastNewStat = ConvertData(LastOrigStat)
                If LastNewStat = -1 Then
                    Throw New Exception("Exception running ConvertData")
                End If

                'Write DataTable tblNewStatus into destination Machine_Status
                Using bulkCopyStatus As SqlBulkCopy = New SqlBulkCopy(strConnNew, SqlBulkCopyOptions.KeepIdentity)
                    bulkCopyStatus.DestinationTableName = "dbo.[Machine_Status]"
                    bulkCopyStatus.WriteToServer(tblNewStatus)
                End Using

                'Write DataTable tblNewXVICond into destination EquipCond_RST-XVI
                Using bulkCopyCond As SqlBulkCopy = New SqlBulkCopy(strConnNew) ', SqlBulkCopyOptions.KeepIdentity)
                    bulkCopyCond.DestinationTableName = "dbo.[EquipCond_RST-XVI]"
                    bulkCopyCond.WriteToServer(tblNewXVICond)
                End Using

                'Write DataTable tblNewStops into destination Stops
                Using bulkCopyStops As SqlBulkCopy = New SqlBulkCopy(strConnNew) ', SqlBulkCopyOptions.KeepIdentity)
                    bulkCopyStops.DestinationTableName = "dbo.[Stops]"
                    bulkCopyStops.WriteToServer(tblNewStops)
                End Using

                'The LastOrigStat value returned is equal to the Max in the table
                'All done. If not, reseed and repeat.
                If LastOrigStat = MaxOrigStat Then
                    AllDone = True
                    Call Write2Log("Main", "Last Original StatusID processed was: " & LastOrigStat.ToString, "Write this down!")
                    Debug.Print("Last Original StatusID processed was: " & LastOrigStat.ToString)
                    MsgBox("Last Original StatusID processed was: " & LastOrigStat.ToString, MsgBoxStyle.Information, "Write this down!")
                Else
                    tblNewXVICond.Clear()
                    tblNewStops.Clear()
                    tblNewStatus.Clear()
                    tblNewStatus.Columns(0).AutoIncrementSeed = LastNewStat + 1
                End If
                RunningCount += BatchSize
                TSpan2 = TimeSpan.FromSeconds(Int(ProgRun.Elapsed.TotalSeconds))
                Console.CursorLeft = 0 : Console.CursorTop = 7
                Console.Write("Total Time: " & TSpan2.ToString)
            Loop Until AllDone = True
            Console.CursorLeft = 0 : Console.CursorTop = 8
            Console.WriteLine("Process Comnplete. Last StatusID processed was: " & LastOrigStat.ToString)
            Console.Write("Press any key to end program.")
            Console.ReadKey()
            Console.Clear()

        Catch ex As Exception
            MsgBox(ex.Message)
            Call Write2Log("Main", "", ex.Message)
        End Try
    End Sub

    Function ConvertData(ByRef LastOrigStat As Long) As Long
        'Receives starting Original StatusID
        'Returns the last new AutoIdendity value created for seeding purposes.
        ConvertData = -1
        Dim sw As New Stopwatch
        Dim RecordCount As Long
        Dim intOrigStatusID, intNewStatusID As Long

        Dim FStops As DataRow()
        Dim dbDA_Orig As SqlDataAdapter
        Dim dbDT_Orig As DataTable
        Dim dbCmdRead As SqlCommand
        Dim dbConnOld, dbConnNew As SqlConnection

        Dim strReadOrigSQL As String
        Dim n, i As Long
        Dim TSpan1 As TimeSpan
        Try
            sw.Start()
            TSpan1 = TimeSpan.FromSeconds(0)

            'Generate SQL strings
            'Read all rows from Original Status_RST-XVI table
            'strReadOrigSQL = "SELECT TOP (" & BatchSize.ToString & ") * FROM [Status_RST-XVI] Where (Stamp >= CONVERT(DATETIME, '2019-04-01 00:00:00', 102)) AND " _
            '            & "(StatusID > " & LastOrigStat.ToString & ") ORDER BY STAMP ASC;"
            strReadOrigSQL = "SELECT TOP (" & BatchSize.ToString & ") * FROM [Status_RST-XVI] WHERE " _
                        & "(StatusID > " & LastOrigStat.ToString & ") ORDER BY StatusID ASC;"

            'Manage SQL Connections
            dbConnOld = New SqlConnection(strConnOld)
            dbConnNew = New SqlConnection(strConnNew)
            dbConnOld.Open()
            dbConnNew.Open()

            'Assign SQLCommand Objects their CommandText and Connection information
            dbCmdRead = New SqlCommand(strReadOrigSQL, dbConnOld) : dbCmdRead.CommandTimeout = 0

            'Start by filling Datatable with Original Status_RST-XVI rows
            dbDT_Orig = New DataTable
            dbDA_Orig = New SqlDataAdapter(dbCmdRead)
            RecordCount = dbDA_Orig.Fill(dbDT_Orig)
            If RecordCount = 0 Then
                Throw New Exception("Invalid RecordCount from initial dbDT_Orig datatable fill command.")
            End If

            'Start stopwatch for timing purposes
            'Console.Clear()
            Console.CursorLeft = 0 : Console.CursorTop = 5
            Console.Write(Space(40)) '"Batch record: " & Format(n, "N0") & " of " & Format(BatchSize, "N0"))

            For Each row In dbDT_Orig.Rows
                n = n + 1
                If n Mod 100 = 0 Then
                    Console.CursorLeft = 0 : Console.CursorTop = 5
                    Console.Write("Batch record: " & Format(n, "N0") & " of " & Format(BatchSize, "N0"))

                    'TSpan1 = TimeSpan.FromSeconds(Int(sw.Elapsed.TotalSeconds))
                    'If n > 10000 Then
                    'TSpan2 = TimeSpan.FromSeconds(Int(((RecordCount - n) * sw.Elapsed.TotalSeconds) / n))
                    'End If
                    'Console.CursorLeft = 0
                    'Console.CursorTop = 2
                    'Console.WriteLine("Total Active Records: " & Format(RecordCount, "N0")) ' Records
                    'Console.WriteLine("Records Processed:    " & Format(n, "N0")) ' Records Processed
                    'Console.WriteLine("Time Elapsed:" & vbTab & TSpan1.ToString) ' Elapsed Time
                    'Console.WriteLine("Time Remaining:" & vbTab & TSpan2.ToString) ' Time Remaining
                End If

                Try
                    'Common variables likely to be used:
                    intOrigStatusID = row("StatusID")
                    intNewStatusID = AddStatusRow(row)

                    'Make New Machine_Status entry
                    If intNewStatusID = 0 Then
                        Throw New Exception("Exception in AddStatusRow on original StatusID: " & row("StatusID").ToString)
                    End If

                    'Make New EquipCond_RST-XVI entry
                    If AddXVICondRow(row, intNewStatusID) = 0 Then
                        Throw New Exception("Exception in AddStatusRow on original StatusID: " & row("StatusID").ToString)
                    End If

                    'Now, work on the Stops....
                    If row("MachFault") = True Or row("OpStop") = True Then
                        FStops = dbDT_Stops.Select("StatusID = " & intOrigStatusID.ToString)
                        For i = 0 To FStops.GetUpperBound(0)
                            If AddNewStopsRow(FStops(i), intNewStatusID) = 0 Then

                            End If
                        Next
                    End If
                    'This assigns the newly created StatusID in the desitnation table to the function return value.
                    ConvertData = intNewStatusID
                    LastOrigStat = intOrigStatusID

                Catch ex As Exception
                    Call Write2Log("ConvertData", "", ex.Message)
                End Try
            Next
            TSpan1 = TimeSpan.FromSeconds(Int(sw.Elapsed.TotalSeconds))
            'Debug.Print(TSpan1.TotalSeconds.ToString)
            Console.CursorLeft = 0 : Console.CursorTop = 6
            Console.Write("Batch Time: " & TSpan1.ToString)

        Catch ex As Exception
            MsgBox("Exception Occurred in processing ConvertData.")
            Call Write2Log("ConvertData", "", ex.Message)
        End Try
    End Function

    Function SetConnections() As Boolean
        SetConnections = False
        Try
            'Define connection strings
            'Source Database
            'strConnOld = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=15;"
            'strConnOld = "Data Source=CTENG02\ENGSQL2014;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"
            strConnOld = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"
            'strConnOld = "Data Source=RUSSELLDESKTOP\SQLEXPRESS;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"

            'Destination Database
            'strConnNew = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"
            'strConnNew = "Data Source=CTENG02\ENGSQL2017;Initial Catalog=DCAP_Data;Trusted_Connection=Yes;Connection Timeout=30;"
            strConnNew = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=DCAP_Data;Trusted_Connection=Yes;Connection Timeout=30;"
            'strConnNew = "Data Source=RUSSELLDESKTOP\SQLEXPRESS;Initial Catalog=DCAP_Data;Trusted_Connection=Yes;Connection Timeout=30;"

            SetConnections = True
        Catch ex As Exception
            Call Write2Log("SetConnections", "", ex.Message)
        End Try
    End Function

    Sub Write2Log(ProcName As String, Info As String, message As String)
        Dim strLogFile As String
        strLogFile = FileIO.FileSystem.CurrentDirectory.ToString & "\" & My.Application.Info.AssemblyName & ".log"
        'If Not File.Exists(strLogFile) Then
        Using writer As New StreamWriter(strLogFile, True)
            writer.WriteLine(Now().ToString & vbTab & "Procedure: " & Trim(ProcName))
            If Not String.IsNullOrEmpty(Trim(Info)) Then
                writer.WriteLine(vbTab & Trim(Info))
            End If
            If Not String.IsNullOrEmpty(Trim(message)) Then
                writer.WriteLine(vbTab & Trim(message))
            End If
            writer.Close()
        End Using
        'End If
    End Sub

    Public Function MakeStatusTable(Seed As Long) As Boolean
        MakeStatusTable = False
        ' Create a new DataTable named NewProducts.
        Try
            tblNewStatus = New DataTable("NewStatus")

            ' Add column objects to the table.
            Dim StatusID As DataColumn = New DataColumn()
            StatusID.DataType = System.Type.GetType("System.Int64")
            StatusID.ColumnName = "StatusID"
            StatusID.AutoIncrement = True
            StatusID.AutoIncrementSeed = Seed
            StatusID.AutoIncrementStep = 1
            tblNewStatus.Columns.Add(StatusID)

            Dim Stamp As DataColumn = New DataColumn()
            Stamp.DataType = System.Type.GetType("System.DateTime")
            Stamp.ColumnName = "Stamp"
            tblNewStatus.Columns.Add(Stamp)

            Dim Comms As DataColumn = New DataColumn()
            Comms.DataType = System.Type.GetType("System.Boolean")
            Comms.ColumnName = "Comms"
            tblNewStatus.Columns.Add(Comms)

            Dim WCID As DataColumn = New DataColumn()
            WCID.DataType = System.Type.GetType("System.Int16")
            WCID.ColumnName = "WCID"
            tblNewStatus.Columns.Add(WCID)

            Dim Power As DataColumn = New DataColumn()
            Power.DataType = System.Type.GetType("System.Boolean")
            Power.ColumnName = "Power"
            tblNewStatus.Columns.Add(Power)

            Dim ProdMode As DataColumn = New DataColumn()
            ProdMode.DataType = System.Type.GetType("System.Boolean")
            ProdMode.ColumnName = "ProdMode"
            tblNewStatus.Columns.Add(ProdMode)

            Dim JobNumber As DataColumn = New DataColumn()
            JobNumber.DataType = System.Type.GetType("System.Int32")
            JobNumber.ColumnName = "JobNumber"
            JobNumber.AllowDBNull = True
            tblNewStatus.Columns.Add(JobNumber)

            Dim JobQty As DataColumn = New DataColumn()
            JobQty.DataType = System.Type.GetType("System.Int32")
            JobQty.ColumnName = "JobQty"
            JobQty.AllowDBNull = True
            tblNewStatus.Columns.Add(JobQty)

            Dim CurrQty As DataColumn = New DataColumn()
            CurrQty.DataType = System.Type.GetType("System.Int32")
            CurrQty.ColumnName = "CurrQty"
            CurrQty.AllowDBNull = True
            tblNewStatus.Columns.Add(CurrQty)

            Dim Running As DataColumn = New DataColumn()
            Running.DataType = System.Type.GetType("System.Boolean")
            Running.ColumnName = "Running"
            tblNewStatus.Columns.Add(Running)

            Dim MachFault As DataColumn = New DataColumn()
            MachFault.DataType = System.Type.GetType("System.Boolean")
            MachFault.ColumnName = "MachFault"
            tblNewStatus.Columns.Add(MachFault)

            Dim MachFaultAck As DataColumn = New DataColumn()
            MachFaultAck.DataType = System.Type.GetType("System.Boolean")
            MachFaultAck.ColumnName = "MachFaultAck"
            tblNewStatus.Columns.Add(MachFaultAck)

            Dim OpStop As DataColumn = New DataColumn()
            OpStop.DataType = System.Type.GetType("System.Boolean")
            OpStop.ColumnName = "OpStop"
            tblNewStatus.Columns.Add(OpStop)

            Dim Activity As DataColumn = New DataColumn()
            Activity.DataType = System.Type.GetType("System.Boolean")
            Activity.ColumnName = "Activity"
            tblNewStatus.Columns.Add(Activity)

            Dim Speed As DataColumn = New DataColumn()
            Speed.DataType = System.Type.GetType("System.Int16")
            Speed.ColumnName = "Speed"
            tblNewStatus.Columns.Add(Speed)

            Dim ElapsedTime As DataColumn = New DataColumn()
            ElapsedTime.DataType = System.Type.GetType("System.Int32")
            ElapsedTime.ColumnName = "ElapsedTime"
            ElapsedTime.AllowDBNull = True
            tblNewStatus.Columns.Add(ElapsedTime)

            MakeStatusTable = True
        Catch ex As Exception
            MakeStatusTable = False
            Call Write2Log("MakeStatusTable", "", ex.Message)
            MsgBox(ex.Message, MsgBoxStyle.Critical, "Exception in function MakeStatusTable")
        End Try
    End Function

    Function SeedStatusTable() As Boolean
        SeedStatusTable = False
        Dim tmpSQL As String = "Select MAX([StatusID]) FROM [Machine_Status];"
        Dim dbConn As SqlConnection
        Dim OldMax As Long
        Try
            dbConn = New SqlConnection(strConnNew)
            dbConn.Open()

            Dim cmdSeed As New SqlCommand(tmpSQL, dbConn)

            If IsDBNull(cmdSeed.ExecuteScalar()) Then
                OldMax = 0
            Else
                OldMax = cmdSeed.ExecuteScalar()
            End If

            tblNewStatus.Clear()
            tblNewStatus.Columns(0).AutoIncrementSeed = OldMax + 1

            SeedStatusTable = True
        Catch ex As Exception
            MsgBox(ex.Message)
            Call Write2Log("SeedStatusTable", "OldMax=" & OldMax.ToString, ex.Message)
        End Try
    End Function

    Public Function MakeXVICondTable() As Boolean
        MakeXVICondTable = False
        ' Create a new DataTable named NewProducts.
        Try
            tblNewXVICond = New DataTable("NewXVICond")
            ' Add column objects to the table.
            Dim ECID As DataColumn = New DataColumn()
            ECID.DataType = System.Type.GetType("System.Int64")
            ECID.ColumnName = "ECID"
            ECID.AutoIncrement = True
            ECID.AutoIncrementSeed = 1
            ECID.AutoIncrementStep = 1
            tblNewXVICond.Columns.Add(ECID)

            Dim StatusID As DataColumn = New DataColumn()
            StatusID.DataType = System.Type.GetType("System.Int64")
            StatusID.ColumnName = "StatusID"
            tblNewXVICond.Columns.Add(StatusID)

            Dim TempMotor As DataColumn = New DataColumn()
            TempMotor.DataType = System.Type.GetType("System.Int16")
            TempMotor.ColumnName = "TempMotor"
            tblNewXVICond.Columns.Add(TempMotor)

            Dim TempGearBox As DataColumn = New DataColumn()
            TempGearBox.DataType = System.Type.GetType("System.Int16")
            TempGearBox.ColumnName = "TempGearBox"
            tblNewXVICond.Columns.Add(TempGearBox)

            Dim TempFeeder As DataColumn = New DataColumn()
            TempFeeder.DataType = System.Type.GetType("System.Int32")
            TempFeeder.ColumnName = "TempFeeder"
            TempFeeder.AllowDBNull = True
            tblNewXVICond.Columns.Add(TempFeeder)

            Dim TempIndexer As DataColumn = New DataColumn()
            TempIndexer.DataType = System.Type.GetType("System.Int32")
            TempIndexer.ColumnName = "TempIndexer"
            TempIndexer.AllowDBNull = True
            tblNewXVICond.Columns.Add(TempIndexer)

            MakeXVICondTable = True
        Catch ex As Exception
            MakeXVICondTable = False
            Call Write2Log("MakeXVICondTable", "", ex.Message)
            MsgBox(ex.Message, MsgBoxStyle.Critical, "Exception in function MakeXVICondTable")
        End Try
    End Function

    Public Function MakeStopsTable() As Boolean
        MakeStopsTable = False
        ' Create a new DataTable named NewProducts.
        Try
            tblNewStops = New DataTable("NewStops")
            ' Add column objects to the table.
            Dim StopID As DataColumn = New DataColumn()
            StopID.DataType = System.Type.GetType("System.Int64")
            StopID.ColumnName = "StopID"
            StopID.AutoIncrement = True
            StopID.AutoIncrementSeed = 1
            StopID.AutoIncrementStep = 1
            tblNewStops.Columns.Add(StopID)

            Dim StatusID As DataColumn = New DataColumn()
            StatusID.DataType = System.Type.GetType("System.Int32")
            StatusID.ColumnName = "StatusID"
            tblNewStops.Columns.Add(StatusID)

            Dim MStop As DataColumn = New DataColumn()
            MStop.DataType = System.Type.GetType("System.Boolean")
            MStop.ColumnName = "MStop"
            tblNewStops.Columns.Add(MStop)

            Dim OStop As DataColumn = New DataColumn()
            OStop.DataType = System.Type.GetType("System.Int16")
            OStop.ColumnName = "OStop"
            tblNewStops.Columns.Add(OStop)

            Dim MStopCode As DataColumn = New DataColumn()
            MStopCode.DataType = System.Type.GetType("System.Int32")
            MStopCode.ColumnName = "MStopCode"
            MStopCode.AllowDBNull = True
            tblNewStops.Columns.Add(MStopCode)

            Dim OStopCode As DataColumn = New DataColumn()
            OStopCode.DataType = System.Type.GetType("System.Int32")
            OStopCode.ColumnName = "OStopCode"
            OStopCode.AllowDBNull = True
            tblNewStops.Columns.Add(OStopCode)

            Dim StopCode As DataColumn = New DataColumn()
            StopCode.DataType = System.Type.GetType("System.Int32")
            StopCode.ColumnName = "StopCode"
            StopCode.AllowDBNull = True
            tblNewStops.Columns.Add(StopCode)

            MakeStopsTable = True
        Catch ex As Exception
            MakeStopsTable = False
            Call Write2Log("MakeStopsTable", "", ex.Message)
            MsgBox(ex.Message, MsgBoxStyle.Critical, "Exception in function MakeStopsTable")
        End Try
    End Function

    Private Function AddStatusRow(appendRow As DataRow) As Long
        AddStatusRow = 0
        Dim newStatusRow As DataRow
        Try
            newStatusRow = tblNewStatus.NewRow()
            newStatusRow("Stamp") = appendRow("Stamp")
            newStatusRow("Comms") = appendRow("Comms")
            newStatusRow("WCID") = appendRow("WCID")
            newStatusRow("Power") = appendRow("Power")
            newStatusRow("ProdMode") = appendRow("ProdMode")
            newStatusRow("JobNumber") = appendRow("JobNumber")
            newStatusRow("JobQty") = appendRow("JobQty")
            newStatusRow("CurrQty") = appendRow("CurrQty")
            newStatusRow("Running") = appendRow("Running")
            newStatusRow("MachFault") = appendRow("MachFault")
            newStatusRow("MachFaultAck") = appendRow("MachFaultAck")
            newStatusRow("OpStop") = appendRow("OpStop")
            newStatusRow("Activity") = appendRow("Activity")
            newStatusRow("Speed") = appendRow("Speed")
            newStatusRow("ElapsedTime") = appendRow("ElapsedTime")
            tblNewStatus.Rows.Add(newStatusRow)
            tblNewStatus.AcceptChanges()
            AddStatusRow = newStatusRow("StatusID")
        Catch ex As Exception
            AddStatusRow = 0
            Call Write2Log("AddStatusRow", "", ex.Message)
            MsgBox(ex.Message, MsgBoxStyle.Critical, "Exception in function AddStatusRow")
        End Try
    End Function

    Function AddXVICondRow(appendRow As DataRow, NewID As Long) As Long
        AddXVICondRow = 0
        Dim newCondRow As DataRow
        Try
            newCondRow = tblNewXVICond.NewRow()
            newCondRow("StatusID") = NewID
            newCondRow("TempMotor") = appendRow("TempMotor")
            newCondRow("TempGearBox") = appendRow("TempGearBox")
            newCondRow("TempFeeder") = appendRow("TempFeeder")
            newCondRow("TempIndexer") = appendRow("TempIndexer")
            tblNewXVICond.Rows.Add(newCondRow)
            tblNewXVICond.AcceptChanges()
            AddXVICondRow = newCondRow("ECID")
        Catch ex As Exception
            AddXVICondRow = 0
            Call Write2Log("AddXVICondRow", "", ex.Message)
            MsgBox(ex.Message, MsgBoxStyle.Critical, "Exception in function AddXVICondRow")
        End Try
    End Function

    Function AddNewStopsRow(appendRow As DataRow, NewID As Long) As Long
        AddNewStopsRow = 0
        Dim newStopRow As DataRow
        Dim AggCode As Integer
        Try
            If Not appendRow("MStopCode") = 0 Then
                AggCode = appendRow("MStopCode")
            Else
                If Not appendRow("OStopCode") = 0 Then
                    AggCode = appendRow("OStopCode")
                Else
                    AggCode = 0
                End If
            End If
            newStopRow = tblNewStops.NewRow()
            newStopRow("StatusID") = NewID
            newStopRow("MStop") = appendRow("MStop")
            newStopRow("OStop") = appendRow("OStop")
            newStopRow("MStopCode") = appendRow("MStopCode")
            newStopRow("OStopCode") = appendRow("OStopCode")
            newStopRow("StopCode") = AggCode
            tblNewStops.Rows.Add(newStopRow)
            tblNewStops.AcceptChanges()
            AddNewStopsRow = newStopRow("StopID")
        Catch ex As Exception
            AddNewStopsRow = 0
            Call Write2Log("AddNewStopsRow", "", ex.Message)
            MsgBox(ex.Message, MsgBoxStyle.Critical, "Exception in function AddNewStopsRow")
        End Try
    End Function

End Module
