Imports System.Data.SqlClient
Imports System.IO
Imports System.Threading

Module Module1
    Public tblNewStatus As DataTable
    Public tblNewXVICond As DataTable
    Public tblNewStops As DataTable

    Sub Main()
        Dim sw As New Stopwatch
        Dim RecordCount, StopCount As Long

        Dim dbDT_Orig, dbDT_Stops As DataTable
        Dim FStops As DataRow()
        Dim dbDA_Orig, dbDA_Stops As SqlDataAdapter
        Dim dbCmdRead, dbCmdRStops, dbCmdWIDList As SqlCommand
        Dim dbConnOld, dbConnNew As SqlConnection

        Dim strReadOrigSQL, strReadStopsSQL, strWriteStatusIDConverted As String
        Dim strConnOld, strConnNew, strKeyVal As String
        Dim n, i, KeyVal As Long
        Dim TSpan1, TSpan2 As TimeSpan

        'Define connection strings
        'strConnOld = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=15;"
        'strConnOld = "Data Source=CTENG02\ENGSQL2014;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"
        strConnOld = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"
        'strConnOld = "Data Source=RUSSELLDESKTOP\SQLEXPRESS;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"

        'strConnNew = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"
        'strConnNew = "Data Source=CTENG02\ENGSQL2017;Initial Catalog=DCAP_Data;Trusted_Connection=Yes;Connection Timeout=30;"
        strConnNew = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=DCAP_Data;Trusted_Connection=Yes;Connection Timeout=30;"
        'strConnNew = "Data Source=RUSSELLDESKTOP\SQLEXPRESS;Initial Catalog=DCAP_Data;Trusted_Connection=Yes;Connection Timeout=30;"

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

        'Generate SQL strings
        'Read all rows from Original Status_RST-XVI table
        strReadOrigSQL = "SELECT top(1000) * FROM [Status_RST-XVI] Where (Stamp >= CONVERT(DATETIME, '2019-04-01 00:00:00', 102)) AND (StatusID > " & KeyVal.ToString & ") ORDER BY STAMP ASC;"
        'Read all Stops for a given StatusID
        'strReadStopsSQL = "SELECT * FROM [Stops] WHERE (StatusID = @OrigStatusID) "
        'Read all original stops
        strReadStopsSQL = "SELECT * FROM [Stops] ;"
        'Update the StatusIDList for the selected StatusID to be Converted=true
        strWriteStatusIDConverted = "UPDATE StatusIDList SET [Converted] = 1 WHERE ([StatusID] = 0 );"

        'Manage SQL Connections
        dbConnOld = New SqlConnection(strConnOld)
        dbConnNew = New SqlConnection(strConnNew)
        dbConnOld.Open()
        dbConnNew.Open()

        'Assign SQLCommand Objects their CommandText and Connection information
        dbCmdRead = New SqlCommand(strReadOrigSQL, dbConnOld) : dbCmdRead.CommandTimeout = 0
        dbCmdRStops = New SqlCommand(strReadStopsSQL, dbConnOld) : dbCmdRStops.CommandTimeout = 0
        dbCmdWIDList = New SqlCommand(strWriteStatusIDConverted, dbConnOld) : dbCmdWIDList.CommandTimeout = 0

        Console.WriteLine("Preparing Queries...")

        'Start by filling Datatable with Original Status_RST-XVI rows
        dbDT_Orig = New DataTable
        dbDA_Orig = New SqlDataAdapter(dbCmdRead)
        RecordCount = dbDA_Orig.Fill(dbDT_Orig)
        If RecordCount = 0 Then
            Throw New Exception("Invalid RecordCount from initial dbDT_Orig datatable fill command.")
        Else
            'Console.WriteLine("Total Status Records: " & Format(RecordCount, "N0"))
        End If

        'Fill the Stops data table wit the original stops
        'Prepares objects for Stops processing later on
        dbDT_Stops = New DataTable
        dbDA_Stops = New SqlDataAdapter(dbCmdRStops)
        StopCount = dbDA_Stops.Fill(dbDT_Stops)
        If StopCount = 0 Then
            Throw New Exception("Invalid RecordCount from initial dbDT_Stops datatable fill command.")
        Else
            'Console.WriteLine("Total Stops: " & Format(StopCount, "N0"))
        End If

        'Structure the three new working DataTables
        'tblNewStatus, tblNewXVICond, tblNewStops
        If Not MakeStatusTable() = True Then
            Throw New Exception("Exception making tblNewStatus.")
        End If
        If Not MakeXVICondTable() = True Then
            Throw New Exception("Exception making tblNewXVICond.")
        End If
        If Not MakeStopsTable() = True Then
            Throw New Exception("Exception making tblNewStops.")
        End If

        'Start stopwatch for timing purposes
        Console.Clear()
        sw.Start()
        TSpan1 = TimeSpan.FromSeconds(0)

        Dim intOrigStatusID, intNewStatusID As Long
        For Each row In dbDT_Orig.Rows
            n = n + 1
            'Console.CursorLeft = 0 : Console.CursorTop = 8
            'Console.WriteLine("Processing record: " & Format(n, "N0"))
            If n Mod 500 = 0 Then
                TSpan1 = TimeSpan.FromSeconds(Int(sw.Elapsed.TotalSeconds))
                If n > 10000 Then
                    TSpan2 = TimeSpan.FromSeconds(Int(((RecordCount - n) * sw.Elapsed.TotalSeconds) / n))
                End If
                Console.CursorLeft = 0
                Console.CursorTop = 2
                Console.WriteLine("Total Active Records: " & Format(RecordCount, "N0")) ' Records
                Console.WriteLine("Records Processed:    " & Format(n, "N0")) ' Records Processed
                Console.WriteLine("Time Elapsed:" & vbTab & TSpan1.ToString) ' Elapsed Time
                Console.WriteLine("Time Remaining:" & vbTab & TSpan2.ToString) ' Time Remaining
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
            Catch ex As Exception

            End Try

        Next

        Using bulkCopy As SqlBulkCopy = New SqlBulkCopy(dbConnNew)
            bulkCopy.DestinationTableName = "dbo.[Machine_Status]"
            Try
                bulkCopy.WriteToServer(tblNewStatus)
            Catch ex As Exception
                MsgBox(ex.Message)
            End Try

            bulkCopy.DestinationTableName = "dbo.[EquipCond_RST-XVI]"
            bulkCopy.WriteToServer(tblNewXVICond)
            Try
                bulkCopy.WriteToServer(tblNewXVICond)
            Catch ex As Exception
                MsgBox(ex.Message)
            End Try

            bulkCopy.DestinationTableName = "dbo.[Stops]"
            Try
                bulkCopy.WriteToServer(tblNewStops)
            Catch ex As Exception
                MsgBox(ex.Message)
            End Try
        End Using

        Console.Clear()
    End Sub

    Sub Write2Log(message As String)
        Dim strLogFile As String
        strLogFile = FileIO.FileSystem.CurrentDirectory.ToString & "\" & My.Application.Info.AssemblyName & ".log"
        'Checks for existence of INI_File string
        If Not File.Exists(strLogFile) Then
            Using writer As New StreamWriter(strLogFile, True)
                writer.WriteLine(Now().ToString)
                writer.WriteLine(vbCrLf)
                writer.Close()
            End Using
        End If
        'Writes info
        Using writer As New StreamWriter(strLogFile, True)
            writer.WriteLine(message)
            writer.Close()
        End Using
    End Sub

    Public Function MakeStatusTable() As Boolean
        MakeStatusTable = False
        ' Create a new DataTable named NewProducts.
        Try
            tblNewStatus = New DataTable("NewStatus")

            ' Add column objects to the table.
            Dim StatusID As DataColumn = New DataColumn()
            StatusID.DataType = System.Type.GetType("System.Int64")
            StatusID.ColumnName = "StatusID"
            StatusID.AutoIncrement = True
            StatusID.AutoIncrementSeed = 1
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
            ElapsedTime.DataType = System.Type.GetType("System.Int16")
            ElapsedTime.ColumnName = "ElapsedTime"
            ElapsedTime.AllowDBNull = True
            tblNewStatus.Columns.Add(ElapsedTime)

            MakeStatusTable = True
        Catch ex As Exception
            MsgBox(ex.Message, MsgBoxStyle.Critical, "Exception in function MakeStatusTable")
            MakeStatusTable = False
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
            ECID.AutoIncrementSeed = 69000
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
            MsgBox(ex.Message, MsgBoxStyle.Critical, "Exception in function MakeXVICondTable")
            MakeXVICondTable = False
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
            MsgBox(ex.Message, MsgBoxStyle.Critical, "Exception in function MakeStopsTable")
            MakeStopsTable = False
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
            Beep()
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
            MsgBox(ex.Message)
            Beep()
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
            MsgBox(ex.Message)
            Beep()
        End Try

    End Function

End Module
