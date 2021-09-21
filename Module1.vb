Imports System.Data.SqlClient
Imports System.IO
Imports System.Threading

Module Module1
    Public tblNewStatus As DataTable
    Public tblNewXVICond As DataTable
    Public tblNewStops As DataTable

    Sub Main()
        Dim sw As New Stopwatch
        Dim RecordCount, StopsCount As Long

        Dim dbDT_Orig, dbDT_Stops, dbDT_FilteredStops As DataTable
        Dim dbDA_Orig, dbDA_Stops As SqlDataAdapter
        Dim dbCmdRead, dbCmdRStops, dbCmdWStat, dbCmdWCond, dbCmdWStop, dbCmdWIDList As SqlCommand
        Dim dbConnOld, dbConnNew As SqlConnection
        Dim Record, StopRow As DataRow
        Dim strReadOrigSQL, strReadStopsSQL, strWriteStatusSQL, strWriteConditionSQL, strWriteStopsSQL, strWriteStatusIDConverted As String
        Dim strConnOld, strConnNew, strMsg, strKeyVal As String
        Dim NewStatusID, TempStatusID, n, i, KeyVal As Long
        Dim TSpan1, TSpan2 As TimeSpan


        Call DoAll()

        'Define connection strings
        'strConnOld = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=15;"
        'strConnOld = "Data Source=CTENG02\ENGSQL2014;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"
        strConnOld = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"
        'strConnOld = "Data Source=RUSSELLDESKTOP\SQLEXPRESS;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"

        'strConnNew = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"
        'strConnNew = "Data Source=CTENG02\ENGSQL2017;Initial Catalog=DCAP_Data;Trusted_Connection=Yes;Connection Timeout=30;"
        strConnNew = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=DCAP_Data;Trusted_Connection=Yes;Connection Timeout=30;"
        'strConnNew = "Data Source=RUSSELLDESKTOP\SQLEXPRESS;Initial Catalog=DCAP_Data;Trusted_Connection=Yes;Connection Timeout=30;"

        'Creates Parameters for database writing
        'Dim Parameter Lists
        Dim SQLStatusParams As New List(Of SqlParameter)
        Dim SQLStopsReadParams As New List(Of SqlParameter)
        Dim SQLStopsParams As New List(Of SqlParameter)
        Dim SQLCondParams As New List(Of SqlParameter)

        'Status Values
        Dim Param100 As New SqlParameter("@Stamp", vbNull)
        Dim Param101 As New SqlParameter("@Comms", False)
        Dim Param102 As New SqlParameter("@WCID", vbNull)
        Dim Param103 As New SqlParameter("@Power", False)
        Dim Param104 As New SqlParameter("@ProdMode", False)
        Dim Param105 As New SqlParameter("@JobNumber", vbNull)
        Dim Param106 As New SqlParameter("@JobQty", vbNull)
        Dim Param107 As New SqlParameter("@CurrQty", vbNull)
        Dim Param108 As New SqlParameter("@Running", False)
        Dim Param109 As New SqlParameter("@MachFault", vbNull)
        Dim Param110 As New SqlParameter("@MachFaultAck", vbNull)
        Dim Param111 As New SqlParameter("@OpStop", vbNull)
        Dim Param112 As New SqlParameter("@Activity", vbNull)
        Dim Param113 As New SqlParameter("@Speed", vbNull)
        Dim Param114 As New SqlParameter("@ET", vbNull)
        SQLStatusParams.Add(Param100) : SQLStatusParams.Add(Param101) : SQLStatusParams.Add(Param102)
        SQLStatusParams.Add(Param103) : SQLStatusParams.Add(Param104) : SQLStatusParams.Add(Param105)
        SQLStatusParams.Add(Param106) : SQLStatusParams.Add(Param107) : SQLStatusParams.Add(Param108)
        SQLStatusParams.Add(Param109) : SQLStatusParams.Add(Param110) : SQLStatusParams.Add(Param111)
        SQLStatusParams.Add(Param112) : SQLStatusParams.Add(Param113) : SQLStatusParams.Add(Param114)

        'For use in STOPS read query
        Dim Param150 As New SqlParameter("@OrigStatusID", vbNull)
        SQLStopsReadParams.Add(Param150)

        'Condition Values
        Dim Param200 As New SqlParameter("@CondStatusID", vbNull)
        Dim Param201 As New SqlParameter("@TempMotor", vbNull)
        Dim Param202 As New SqlParameter("@TempGearBox", vbNull)
        Dim Param203 As New SqlParameter("@TempFeeder", vbNull)
        Dim Param204 As New SqlParameter("@TempIndexer", vbNull)
        SQLCondParams.Add(Param200) : SQLCondParams.Add(Param201) : SQLCondParams.Add(Param202)
        SQLCondParams.Add(Param203) : SQLCondParams.Add(Param204)

        'Stop Values
        Dim Param300 As New SqlParameter("@StopStatusID", vbNull)
        Dim Param301 As New SqlParameter("@MStop", False)
        Dim Param302 As New SqlParameter("@OStop", False)
        Dim Param303 As New SqlParameter("@MStopCode", vbNull)
        Dim Param304 As New SqlParameter("@OStopCode", vbNull)
        Dim Param305 As New SqlParameter("@StopCode", vbNull)
        SQLStopsParams.Add(Param300) : SQLStopsParams.Add(Param301) : SQLStopsParams.Add(Param302)
        SQLStopsParams.Add(Param303) : SQLStopsParams.Add(Param304) : SQLStopsParams.Add(Param305)

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
        strReadOrigSQL = "SELECT top (100000) * FROM [Status_RST-XVI] Where (Stamp >= CONVERT(DATETIME, '2019-04-01 00:00:00', 102)) AND (StatusID > " & KeyVal.ToString & ") ORDER BY STAMP ASC;"
        'Read all Stops for a given StatusID
        strReadStopsSQL = "SELECT * FROM [Stops] WHERE (StatusID = @OrigStatusID) " ' _
        '& "ORDER BY StopID ASC;"
        'Append to new Mach_Status table, collecting the newly created Identity value for use in Condition and Stops
        strWriteStatusSQL = "INSERT INTO [Machine_Status] (Stamp, Comms, WCID, Power, ProdMode, JobNumber, " _
                & "JobQty, CurrQty, Running, MachFault, MachFaultAck, OpStop, Activity, Speed, ElapsedTime) " _
                & "VALUES (@Stamp, @Comms, @WCID, @Power, @ProdMode, @JobNumber, @JobQty, @CurrQty, " _
                & "@Running, @MachFault, @MachFaultAck, @OpStop, @Activity, @Speed, @ET); " _
                & "SELECT SCOPE_IDENTITY();"
        'Append to the new Machine Condition table for the RST-XVI. Only machine in the database now
        strWriteConditionSQL = "INSERT INTO [EquipCond_RST-XVI] (StatusID, TempMotor, TempGearBox, " _
                & "TempFeeder, TempIndexer) " _
                & "VALUES (@CondStatusID, @TempMotor, @TempGearBox, @TempFeeder, @TempIndexer)" _
                & "SELECT SCOPE_IDENTITY();"
        'Append to the unified Stops table all existing Stop entries, referencing the new StatusID returned
        'previously from the SCOPE_IDENTITY in strWriteStatusSQL
        strWriteStopsSQL = "INSERT INTO [Stops] (StatusID, MStop, OStop, MStopCode, OStopCode, StopCode) " _
                & "VALUES (@StopStatusID, @MStop, @OStop, @MStopCode, @OStopCode, @StopCode)" _
                & "SELECT SCOPE_IDENTITY();"
        'Update the StatusIDList for the selected StatusID to be Converted=true
        strWriteStatusIDConverted = "UPDATE StatusIDList SET [Converted] = 1 WHERE ([StatusID] = 0 );"

        'Manage SQL Connections
        dbConnOld = New SqlConnection(strConnOld)
        dbConnNew = New SqlConnection(strConnNew)
        dbConnOld.Open()
        dbConnNew.Open()

        'Assign SQLCommand Objects their CommandText and Connection information
        dbCmdRead = New SqlCommand(strReadOrigSQL, dbConnOld) : dbCmdRead.CommandTimeout = 0
        dbCmdWStat = New SqlCommand(strWriteStatusSQL, dbConnNew) : dbCmdWStat.CommandTimeout = 0
        dbCmdWCond = New SqlCommand(strWriteConditionSQL, dbConnNew) : dbCmdWCond.CommandTimeout = 0
        dbCmdWStop = New SqlCommand(strWriteStopsSQL, dbConnNew) : dbCmdWStop.CommandTimeout = 0
        dbCmdRStops = New SqlCommand(strReadStopsSQL, dbConnOld) : dbCmdRStops.CommandTimeout = 0
        dbCmdWIDList = New SqlCommand(strWriteStatusIDConverted, dbConnOld) : dbCmdWIDList.CommandTimeout = 0

        'Assign Parameters to the Command Objects
        SQLStatusParams.ForEach(Sub(p) dbCmdWStat.Parameters.Add(p))
        SQLCondParams.ForEach(Sub(p) dbCmdWCond.Parameters.Add(p))
        SQLStopsParams.ForEach(Sub(p) dbCmdWStop.Parameters.Add(p))
        SQLStopsReadParams.ForEach(Sub(p) dbCmdRStops.Parameters.Add(p))
        Console.WriteLine("Preparing Queries...")
        Try
            'Prepares objects for Stops processing later on
            dbDT_Stops = New DataTable
            dbDA_Stops = New SqlDataAdapter(dbCmdRStops)

            'Start by filling Datatable with Original Status_RST-XVI rows
            dbDT_Orig = New DataTable
            dbDA_Orig = New SqlDataAdapter(dbCmdRead)
            RecordCount = dbDA_Orig.Fill(dbDT_Orig)
            If RecordCount = 0 Then
                Throw New Exception("Invalid RecordCount from initial dbDT_Orig datatable fill command.")
            End If

            Console.Clear()
            Console.WriteLine("Current Status")
            Threading.Thread.Sleep(500)
            'Start stopwatch for timing purposes
            sw.Start()
            TSpan1 = TimeSpan.FromSeconds(0)


            For n = 0 To dbDT_Orig.Rows.Count - 1
                Record = dbDT_Orig.Rows(n)
                'Status update handling
                If n Mod 100 = 0 Then
                    'Form1.ProgressBar1.Value = n
                    'Form1.TextBox2.Text = Format(n, "N0") ' Records Processed

                    TSpan1 = TimeSpan.FromSeconds(Int(sw.Elapsed.TotalSeconds))
                    'Form1.TextBox3.Text = TSpan1.ToString   'Elapsed Time
                    If n > 100 Then
                        TSpan2 = TimeSpan.FromSeconds(Int(((RecordCount - n) * sw.Elapsed.TotalSeconds) / n))
                        'Form1.TextBox4.Text = TSpan2.ToString
                    End If
                    'Form1.Refresh()
                    'Console.Clear()
                    Console.CursorLeft = 0
                    Console.CursorTop = 2
                    Console.WriteLine("Total Active Records: " & Format(RecordCount, "N0")) ' Records
                    Console.WriteLine("Records Processed:    " & Format(n, "N0")) ' Records Processed
                    Console.WriteLine("Time Elapsed:" & vbTab & TSpan1.ToString) ' Elapsed Time
                    Console.WriteLine("Time Remaining:" & vbTab & TSpan2.ToString) ' Time Remaining
                End If
                'Assign values from Record columns to Parameters
                Param100.Value = Record("Stamp")
                Param101.Value = Record("Comms")
                Param102.Value = Record("WCID")
                Param103.Value = Record("Power")
                Param104.Value = Record("ProdMode")
                Param105.Value = Record("JobNumber")
                Param106.Value = Record("JobQty")
                Param107.Value = Record("CurrQty")
                Param108.Value = Record("Running")
                Param109.Value = Record("MachFault")
                Param110.Value = Record("MachFaultAck")
                Param111.Value = Record("OpStop")
                Param112.Value = Record("Activity")
                Param113.Value = Record("Speed")
                Param114.Value = Record("ElapsedTime")
                Param150.Value = Record("StatusID")
                Param200.Value = 0    'Gets filled in with New ID
                Param201.Value = Record("TempMotor")
                Param202.Value = Record("TempGearBox")
                Param203.Value = Record("TempFeeder")
                Param204.Value = Record("TempIndexer")
                Param300.Value = 0    'Gets filled in with New ID
                Param301.Value = 0    'Query dependent
                Param302.Value = 0    'Query dependent
                Param303.Value = 0    'Query dependent
                Param304.Value = 0    'Query dependent
                Param305.Value = 0    'Query dependent

                'Append data to [Mach_Status] table
                'dbCmdWStat
                'dbCmd = New SqlCommand(strStatusQuery, dbConn)

                'dbConn.Open()
                'Executes Query. If unsuccessful, Exception is pushed.
                NewStatusID = 0
                NewStatusID = dbCmdWStat.ExecuteScalar()
                If NewStatusID = 0 Then
                    Throw New Exception("Invalid return of NewStatusID after dbCmdWStat.ExecuteScalar() writing new Status fields.")
                End If
                Param200.Value = NewStatusID
                Param300.Value = NewStatusID
                'Now a valid NewStatusID is available to link Conditions to
                TempStatusID = 0
                TempStatusID = dbCmdWCond.ExecuteScalar()
                If TempStatusID = 0 Then
                    Throw New Exception("Invalid return of TempStatusID after dbCmdWCond.ExecuteScalar() writing new Condition fields.")
                End If

                'If MachStop or OpStop are true, then a cooresponding record must be added to the [Stops] table
                If Record("MachFault") = True Or Record("OpStop") = True Then
                    Param150.Value = Record("StatusID")
                    dbDT_Stops.Clear()
                    strMsg = ""
                    StopsCount = 0
                    StopsCount = dbDA_Stops.Fill(dbDT_Stops)
                    If StopsCount = 0 Then 'No cooresponding Stops in table for StatusID
                        strMsg = "Machine or Operator Stop with no Stops entered, " & "StatusID:" & Record("StatusID").ToString & ", " _
                            & "MachFault:" & Record("MachFault").ToString & ", " & "OpStop:" & Record("OpStop").ToString
                        Call Write2Log(strMsg)
                        If Record("MachFault") = True Then
                            'There is nothing to do in this case. This conditional is for holding and future use.
                        End If
                        If Record("OpStop") = True Then
                            'Add a Stop record, with StopCode being 0
                            'Stop Values
                            Param301.Value = False  'Not MStop
                            Param302.Value = True   'Is an OStop
                            Param303.Value = 0  'MStopCode
                            Param304.Value = 0  'OStopCode
                            Param305.Value = 0  'Unified Stop Code
                            'Write to Stops table now
                            TempStatusID = 0
                            'TempStatusID = dbCmdWStop.ExecuteScalar()
                            TempStatusID = dbCmdWStop.ExecuteNonQuery
                            If TempStatusID = 0 Then
                                Throw New Exception("Invalid return of TempStatusID after dbCmdWCond.ExecuteScalar() writing new Stop fields.")
                            End If
                        End If
                    Else 'Process Stops data
                        For i = 0 To dbDT_Stops.Rows.Count - 1
                            StopRow = dbDT_Stops.Rows(i)
                            Param301.Value = StopRow("MStop")   'MStop
                            Param302.Value = StopRow("OStop")   'OStop
                            Param303.Value = StopRow("MStopCode")   'MStopCode
                            Param304.Value = StopRow("OStopCode") 'OStopCode
                            If StopRow("Mstop") = True Then
                                Param305.Value = StopRow("MStopCode") 'Unified Code
                            Else
                                Param305.Value = StopRow("OStopCode") 'Unified Code
                            End If
                            'Write to Stops table now
                            TempStatusID = 0
                            TempStatusID = dbCmdWStop.ExecuteScalar()
                            If TempStatusID = 0 Then
                                'Throw New Exception("Invalid return of TempStatusID after dbCmdWCond.ExecuteScalar() writing new Stop fields.")
                            End If

                        Next i
                    End If
                    Debug.WriteLine(vbCrLf)
                End If
                strWriteStatusIDConverted = "UPDATE StatusIDList SET [Converted] = 1 WHERE ([StatusID] = " & Param150.Value.ToString & " );"
                dbCmdWIDList.CommandText = strWriteStatusIDConverted
                RecordCount = dbCmdWIDList.ExecuteNonQuery
                If RecordCount <> 1 Then
                    Throw New Exception("Invalid RecordCount from initial dbDT_Orig datatable fill command.")
                End If
            Next n
            'Form1.ProgressBar1.Value = RecordCount
            'Form1.TextBox2.Text = Format(RecordCount, "N0")
            'Form1.TextBox3.Text = Format((sw.Elapsed).TotalSeconds, "N1") '.ToString
            'Form1.textbox5.text = Record("StatusID").ToString
            TSpan1 = TimeSpan.FromSeconds(Int(sw.Elapsed.TotalSeconds))

            TSpan2 = TimeSpan.Zero
            sw.Stop()
            Console.CursorLeft = 0
            Console.CursorTop = 2
            Console.WriteLine("Total Active Records: " & Format(RecordCount, "N0")) ' Records
            Console.WriteLine("Records Processed:    " & Format(n, "N0")) ' Records Processed
            Console.WriteLine("Time Elapsed:" & vbTab & TSpan1.ToString) ' Elapsed Time
            Console.WriteLine("Time Remaining:" & vbTab & TSpan2.ToString) ' Time Remaining
            Console.WriteLine("Last StatusID processed: " & Record("StatusID").ToString)
            'Threading.Thread.Sleep(1000)
            Console.WriteLine("Press any key to exit.")
            MsgBox("Please write this down:" & vbCrLf & "Last StatusID processed was: " & Record("StatusID").ToString, MsgBoxStyle.Information, "This is why you did this!")
            Console.ReadKey()

        Catch ex As Exception
            MsgBox(ex.Message)
            'MsgBox(ex.Message & vbCrLf & "StatusID:" & Record("StatusID").ToString, MsgBoxStyle.OkOnly, "Exception Warning")

        End Try
        'Threading.Thread.Sleep(500)
    End Sub




    Sub Single_Record_Main()
        Dim sw As New Stopwatch
        Dim RecordCount As Long
        Dim booQ As Boolean
        Dim MaxThd, MaxIO, AvailThd, AvailIO As Integer

        Dim dbDT_IDs As DataTable
        Dim dbDA_IDs As SqlDataAdapter
        Dim dbCmdRIDList As SqlCommand
        Dim dbConnOld As SqlConnection

        Dim strGetStatIDListSQL As String
        Dim strConnOld As String
        Dim n, incMod, intID As Long
        Dim TSpan1, TSpan2 As TimeSpan

        'Define connection strings
        'strConnOld = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=15;"
        'strConnOld = "Data Source=CTENG02\ENGSQL2014;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"
        strConnOld = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"
        'strConnOld = "Data Source=RUSSELLDESKTOP\SQLEXPRESS;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"

        'Generate SQL strings
        'Get list of StatusID's to process
        strGetStatIDListSQL = "SELECT * FROM [StatusIDList] WHERE (StatusID >=95502) AND (Converted = 0);"

        'Manage SQL Connections
        dbConnOld = New SqlConnection(strConnOld)
        dbConnOld.Open()
        'Console.WriteLine("State: {0}", dbConnOld.State)
        'Console.WriteLine("ConnectionTimeout: {0}", dbConnOld.ConnectionTimeout)

        'Assign SQLCommand Objects their CommandText and Connection information
        dbCmdRIDList = New SqlCommand(strGetStatIDListSQL, dbConnOld)

        Try
            'Start by opening the list of StatusID's that have not been converted
            dbDT_IDs = New DataTable
            dbDA_IDs = New SqlDataAdapter(dbCmdRIDList)
            'Console.WriteLine("ConnectionTimeout: {0}", dbDA_IDs.SelectCommand.CommandTimeout)
            'dbDA_IDs.SelectCommand.CommandTimeout = 0
            'Console.WriteLine("ConnectionTimeout: {0}", dbDA_IDs.SelectCommand.CommandTimeout)

            RecordCount = dbDA_IDs.Fill(dbDT_IDs)

            If RecordCount = 0 Then
                Throw New Exception("Invalid RecordCount from initial dbDT_IDs datatable fill command.")
            End If

            'Start stopwatch for timing purposes
            sw.Start()
            TSpan1 = TimeSpan.FromSeconds(0)

            'Start reporting stuff now
            Console.Clear()
            Console.CursorLeft = 0
            Console.CursorTop = 2
            Console.WriteLine("Current Status")
            Console.WriteLine("Total Active Records: " & Format(RecordCount, "N0")) ' Records

            'Setting for reporting interval
            incMod = 10
            'ThreadPool.GetAvailableThreads(Wthd, Pthd)
            'MsgBox("Worker: " & Wthd.ToString)
            'MsgBox("Port: " & Pthd.ToString)

            'ThreadPool.SetMaxThreads(8, 3)

            ThreadPool.GetMaxThreads(MaxThd, MaxIO)
            'MsgBox("Worker: " & Wthd.ToString)
            'MsgBox("Port: " & Pthd.ToString)
            'n = 0
            For Each row In dbDT_IDs.Rows
                'Call ConvertStatus(row("StatusID"))
                intID = row("StatusID")
                'Call ConvertStatus(intID)
                'Do
                'booQ = ThreadPool.QueueUserWorkItem(AddressOf ConvertStatus, intID)
                ThreadPool.QueueUserWorkItem(AddressOf ConvertStatus, intID)
                'ThreadPool.GetAvailableThreads(AvailThd, AvailIO)
                'If MaxThd - AvailThd <= 500 Then
                'Threading.Thread.Sleep(1000)
                'End If
                'Loop Until booQ = True
                'Threading.Thread.Sleep(25)
            Next

            Do
                'n = n + 1
                If n Mod incMod = 0 Then
                    TSpan1 = TimeSpan.FromSeconds(Int(sw.Elapsed.TotalSeconds))
                    If n > incMod Then
                        TSpan2 = TimeSpan.FromSeconds(Int(((RecordCount - n) * sw.Elapsed.TotalSeconds) / n))
                    End If
                    Console.CursorLeft = 0
                    Console.CursorTop = 4
                    Console.WriteLine("Records Processed:    " & Format(n, "N0")) ' Records Processed
                    Console.WriteLine("Time Elapsed:" & vbTab & TSpan1.ToString) ' Elapsed Time
                    Console.WriteLine("Time Remaining:" & vbTab & TSpan2.ToString) ' Time Remaining
                End If



                'Do
                Console.CursorLeft = 5
                Console.CursorTop = 10
                Console.WriteLine("Max Threads: " & MaxThd.ToString & ", " & MaxIO.ToString)
                ThreadPool.GetAvailableThreads(AvailThd, AvailIO)
                Console.CursorLeft = 5
                Console.CursorTop = 11
                Console.WriteLine("Avail Threads: " & AvailThd.ToString & ", " & AvailIO.ToString)
                Console.CursorLeft = 5
                Console.CursorTop = 12
                Console.WriteLine("Used Threads: " & (MaxThd - AvailThd).ToString & ", " & (MaxIO - AvailIO).ToString)
                Thread.Sleep(100)
            Loop

            sw.Stop()
            TSpan2 = TimeSpan.Zero
            Console.CursorLeft = 0
            Console.CursorTop = 4
            Console.WriteLine("Records Processed:    " & Format(n, "N0")) ' Records Processed
            Console.WriteLine("Time Elapsed:" & vbTab & TSpan1.ToString) ' Elapsed Time
            Console.WriteLine("Time Remaining:" & vbTab & TSpan2.ToString) ' Time Remaining
            Console.WriteLine("Press any key to exit.")
            Console.ReadKey()

        Catch ex As Exception
            'MsgBox(ex.Message & vbCrLf & "StatusID:" & Record("StatusID").ToString, MsgBoxStyle.OkOnly, "Exception Warning")
            MsgBox(ex.Message)
        End Try
        'Threading.Thread.Sleep(500)
    End Sub

    Public Sub ConvertStatus(ID As Long)
        Dim RecordCount As Long
        Dim dbDT_IDs, dbDT_Orig, dbDT_Stops As DataTable
        Dim dbDA_IDs, dbDA_Orig, dbDA_Stops As SqlDataAdapter
        Dim Record, StopRow As DataRow
        Dim dbCmdRead, dbCmdRStops, dbCmdWStat, dbCmdWCond, dbCmdWStop, dbCmdWIDList As SqlCommand
        Dim dbConnOld, dbConnNew As SqlConnection

        Dim strReadOrigSQL, strReadStopsSQL, strWriteStatusSQL, strWriteConditionSQL, strWriteStopsSQL, strWriteStatusIDConverted As String

        Dim StopsCount As Long
        Dim strConnOld, strConnNew, strMsg As String
        Dim NewStatusID, TempStatusID, n, i As Long

        'Define connection strings
        'strConnOld = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=15;"
        'strConnOld = "Data Source=CTENG02\ENGSQL2014;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"
        strConnOld = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=0;"
        'strConnOld = "Data Source=RUSSELLDESKTOP\SQLEXPRESS;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"

        'strConnNew = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=ProductionData;Trusted_Connection=Yes;Connection Timeout=30;"
        'strConnNew = "Data Source=CTENG02\ENGSQL2017;Initial Catalog=DCAP_Data;Trusted_Connection=Yes;Connection Timeout=30;"
        strConnNew = "Data Source=CT0000141\SQLEXPRESS_RRR;Initial Catalog=DCAP_Data;Trusted_Connection=Yes;Connection Timeout=0;"
        'strConnNew = "Data Source=RUSSELLDESKTOP\SQLEXPRESS;Initial Catalog=DCAP_Data;Trusted_Connection=Yes;Connection Timeout=30;"

        'Creates Parameters for database writing
        'Dim Parameter Lists
        Dim SQLStatIDListParams As New List(Of SqlParameter)
        Dim SQLStatusParams As New List(Of SqlParameter)
        Dim SQLStopsReadParams As New List(Of SqlParameter)
        Dim SQLStopsParams As New List(Of SqlParameter)
        Dim SQLCondParams As New List(Of SqlParameter)

        'Status Values
        Dim Param100 As New SqlParameter("@Stamp", vbNull)
        Dim Param101 As New SqlParameter("@Comms", False)
        Dim Param102 As New SqlParameter("@WCID", vbNull)
        Dim Param103 As New SqlParameter("@Power", False)
        Dim Param104 As New SqlParameter("@ProdMode", False)
        Dim Param105 As New SqlParameter("@JobNumber", vbNull)
        Dim Param106 As New SqlParameter("@JobQty", vbNull)
        Dim Param107 As New SqlParameter("@CurrQty", vbNull)
        Dim Param108 As New SqlParameter("@Running", False)
        Dim Param109 As New SqlParameter("@MachFault", vbNull)
        Dim Param110 As New SqlParameter("@MachFaultAck", vbNull)
        Dim Param111 As New SqlParameter("@OpStop", vbNull)
        Dim Param112 As New SqlParameter("@Activity", vbNull)
        Dim Param113 As New SqlParameter("@Speed", vbNull)
        Dim Param114 As New SqlParameter("@ET", vbNull)
        SQLStatusParams.Add(Param100) : SQLStatusParams.Add(Param101) : SQLStatusParams.Add(Param102)
        SQLStatusParams.Add(Param103) : SQLStatusParams.Add(Param104) : SQLStatusParams.Add(Param105)
        SQLStatusParams.Add(Param106) : SQLStatusParams.Add(Param107) : SQLStatusParams.Add(Param108)
        SQLStatusParams.Add(Param109) : SQLStatusParams.Add(Param110) : SQLStatusParams.Add(Param111)
        SQLStatusParams.Add(Param112) : SQLStatusParams.Add(Param113) : SQLStatusParams.Add(Param114)

        'For use in STOPS read query
        Dim Param150 As New SqlParameter("@OrigStatusID", vbNull)
        SQLStopsReadParams.Add(Param150)

        'Condition Values
        Dim Param200 As New SqlParameter("@CondStatusID", vbNull)
        Dim Param201 As New SqlParameter("@TempMotor", vbNull)
        Dim Param202 As New SqlParameter("@TempGearBox", vbNull)
        Dim Param203 As New SqlParameter("@TempFeeder", vbNull)
        Dim Param204 As New SqlParameter("@TempIndexer", vbNull)
        SQLCondParams.Add(Param200) : SQLCondParams.Add(Param201) : SQLCondParams.Add(Param202)
        SQLCondParams.Add(Param203) : SQLCondParams.Add(Param204)

        'Stop Values
        Dim Param300 As New SqlParameter("@StopStatusID", vbNull)
        Dim Param301 As New SqlParameter("@MStop", False)
        Dim Param302 As New SqlParameter("@OStop", False)
        Dim Param303 As New SqlParameter("@MStopCode", vbNull)
        Dim Param304 As New SqlParameter("@OStopCode", vbNull)
        Dim Param305 As New SqlParameter("@StopCode", vbNull)
        SQLStopsParams.Add(Param300) : SQLStopsParams.Add(Param301) : SQLStopsParams.Add(Param302)
        SQLStopsParams.Add(Param303) : SQLStopsParams.Add(Param304) : SQLStopsParams.Add(Param305)

        'StatusIDList Values
        'Dim Param400 As New SqlParameter("@IDList", 0)
        'SQLStatIDListParams.Add(Param400)

        'Generate SQL strings
        'Read specific row from Original Status_RST-XVI table
        strReadOrigSQL = "SELECT * FROM [Status_RST-XVI] Where (StatusID = " & ID.ToString & " );"
        'Read all Stops for a given StatusID
        strReadStopsSQL = "SELECT * FROM [Stops] WHERE (StatusID = @OrigStatusID) " ' _
        'Append to new Mach_Status table, collecting the newly created Identity value for use in Condition and Stops
        strWriteStatusSQL = "INSERT INTO [Machine_Status] (Stamp, Comms, WCID, Power, ProdMode, JobNumber, " _
                & "JobQty, CurrQty, Running, MachFault, MachFaultAck, OpStop, Activity, Speed, ElapsedTime) " _
                & "VALUES (@Stamp, @Comms, @WCID, @Power, @ProdMode, @JobNumber, @JobQty, @CurrQty, " _
                & "@Running, @MachFault, @MachFaultAck, @OpStop, @Activity, @Speed, @ET); " _
                & "SELECT SCOPE_IDENTITY();"
        'Append to the new Machine Condition table for the RST-XVI. Only machine in the database now
        strWriteConditionSQL = "INSERT INTO [EquipCond_RST-XVI] (StatusID, TempMotor, TempGearBox, " _
                & "TempFeeder, TempIndexer) " _
                & "VALUES (@CondStatusID, @TempMotor, @TempGearBox, @TempFeeder, @TempIndexer)" _
                & "SELECT SCOPE_IDENTITY();"
        'Append to the unified Stops table all existing Stop entries, referencing the new StatusID returned
        'previously from the SCOPE_IDENTITY in strWriteStatusSQL
        strWriteStopsSQL = "INSERT INTO [Stops] (StatusID, MStop, OStop, MStopCode, OStopCode, StopCode) " _
                & "VALUES (@StopStatusID, @MStop, @OStop, @MStopCode, @OStopCode, @StopCode)" _
                & "SELECT SCOPE_IDENTITY();"
        'Update the StatusIDList for the selected StatusID to be Converted=true
        strWriteStatusIDConverted = "UPDATE StatusIDList SET [Converted] = 1 WHERE ([StatusID] = " & ID.ToString & " );"

        'Manage SQL Connections
        dbConnOld = New SqlConnection(strConnOld)
        dbConnNew = New SqlConnection(strConnNew)
        dbConnOld.Open()
        dbConnNew.Open()

        'Assign SQLCommand Objects their CommandText and Connection information
        dbCmdRead = New SqlCommand(strReadOrigSQL, dbConnOld)
        dbCmdRead.CommandTimeout = 0
        dbCmdWStat = New SqlCommand(strWriteStatusSQL, dbConnNew)
        dbCmdWStat.CommandTimeout = 0
        dbCmdWCond = New SqlCommand(strWriteConditionSQL, dbConnNew)
        dbCmdWCond.CommandTimeout = 0
        dbCmdWStop = New SqlCommand(strWriteStopsSQL, dbConnNew)
        dbCmdWStop.CommandTimeout = 0
        dbCmdRStops = New SqlCommand(strReadStopsSQL, dbConnOld)
        dbCmdRStops.CommandTimeout = 0
        dbCmdWIDList = New SqlCommand(strWriteStatusIDConverted, dbConnOld)
        dbCmdWIDList.CommandTimeout = 0

        'Assign Parameters to the Command Objects
        ' SQLStatusParams.ForEach(Sub(p) dbCmdRead.Parameters.Add(p))
        SQLStatusParams.ForEach(Sub(p) dbCmdWStat.Parameters.Add(p))
        SQLCondParams.ForEach(Sub(p) dbCmdWCond.Parameters.Add(p))
        SQLStopsParams.ForEach(Sub(p) dbCmdWStop.Parameters.Add(p))
        SQLStopsReadParams.ForEach(Sub(p) dbCmdRStops.Parameters.Add(p))

        'Prepares objects for reading the Original Status line
        dbDT_Orig = New DataTable
        dbDA_Orig = New SqlDataAdapter(dbCmdRead)

        'Prepares objects for Stops processing later on
        dbDT_Stops = New DataTable
        dbDA_Stops = New SqlDataAdapter(dbCmdRStops)

        'Prepares objects for updating StatusID list later
        dbDT_IDs = New DataTable
        dbDA_IDs = New SqlDataAdapter(dbCmdWIDList)

        Try

            'Read the single record from Status_RST-XVI
            RecordCount = dbDA_Orig.Fill(dbDT_Orig)
            If RecordCount <> 1 Then
                Throw New Exception("Invalid RecordCount from initial dbDT_Orig datatable fill command.")
            End If
            'Assign single row in DataTable to DataRow Record
            Record = dbDT_Orig.Rows(0)

            'Assign values from Record columns to Parameters
            Param100.Value = Record("Stamp")
            Param101.Value = Record("Comms")
            Param102.Value = Record("WCID")
            Param103.Value = Record("Power")
            Param104.Value = Record("ProdMode")
            Param105.Value = Record("JobNumber")
            Param106.Value = Record("JobQty")
            Param107.Value = Record("CurrQty")
            Param108.Value = Record("Running")
            Param109.Value = Record("MachFault")
            Param110.Value = Record("MachFaultAck")
            Param111.Value = Record("OpStop")
            Param112.Value = Record("Activity")
            Param113.Value = Record("Speed")
            Param114.Value = Record("ElapsedTime")
            Param150.Value = Record("StatusID")
            Param200.Value = 0    'Gets filled in with New ID
            Param201.Value = Record("TempMotor")
            Param202.Value = Record("TempGearBox")
            Param203.Value = Record("TempFeeder")
            Param204.Value = Record("TempIndexer")
            Param300.Value = 0    'Gets filled in with New ID
            Param301.Value = 0    'Query dependent
            Param302.Value = 0    'Query dependent
            Param303.Value = 0    'Query dependent
            Param304.Value = 0    'Query dependent
            Param305.Value = 0    'Query dependent

            'Executes Query. If unsuccessful, Exception is pushed.
            NewStatusID = 0
            NewStatusID = dbCmdWStat.ExecuteScalar()
            If NewStatusID = 0 Then
                Throw New Exception("Invalid return of NewStatusID after dbCmdWStat.ExecuteScalar() writing new Status fields.")
            End If
            Param200.Value = NewStatusID
            Param300.Value = NewStatusID
            'Now a valid NewStatusID is available to link Conditions to
            TempStatusID = 0
            TempStatusID = dbCmdWCond.ExecuteScalar()
            If TempStatusID = 0 Then
                Throw New Exception("Invalid return of TempStatusID after dbCmdWCond.ExecuteScalar() writing new Condition fields.")
            End If

            'If MachStop or OpStop are true, then a cooresponding record must be added to the [Stops] table
            If Record("MachFault") = True Or Record("OpStop") = True Then
                Param150.Value = Record("StatusID")
                dbDT_Stops.Clear()
                strMsg = ""
                StopsCount = 0
                StopsCount = dbDA_Stops.Fill(dbDT_Stops)
                If StopsCount = 0 Then 'No cooresponding Stops in table for StatusID
                    strMsg = "Machine or Operator Stop with no Stops entered, " & "StatusID:" & Record("StatusID").ToString & ", " _
                        & "MachFault:" & Record("MachFault").ToString & ", " & "OpStop:" & Record("OpStop").ToString
                    Call Write2Log(strMsg)
                    If Record("MachFault") = True Then
                        'There is nothing to do in this case. This conditional is for holding and future use.
                    End If
                    If Record("OpStop") = True Then
                        'Add a Stop record, with StopCode being 0
                        'Stop Values
                        Param301.Value = False  'Not MStop
                        Param302.Value = True   'Is an OStop
                        Param303.Value = 0  'MStopCode
                        Param304.Value = 0  'OStopCode
                        Param305.Value = 0  'Unified Stop Code
                        'Write to Stops table now
                        TempStatusID = 0
                        'TempStatusID = dbCmdWStop.ExecuteScalar()
                        TempStatusID = dbCmdWStop.ExecuteNonQuery
                        If TempStatusID = 0 Then
                            Throw New Exception("Invalid return of TempStatusID after dbCmdWCond.ExecuteScalar() writing new Stop fields.")
                        End If
                    End If
                Else 'Process Stops data
                    For i = 0 To dbDT_Stops.Rows.Count - 1
                        StopRow = dbDT_Stops.Rows(i)
                        Param301.Value = StopRow("MStop")   'MStop
                        Param302.Value = StopRow("OStop")   'OStop
                        Param303.Value = StopRow("MStopCode")   'MStopCode
                        Param304.Value = StopRow("OStopCode") 'OStopCode
                        If StopRow("Mstop") = True Then
                            Param305.Value = StopRow("MStopCode") 'Unified Code
                        Else
                            Param305.Value = StopRow("OStopCode") 'Unified Code
                        End If
                        'Write to Stops table now
                        TempStatusID = 0
                        TempStatusID = dbCmdWStop.ExecuteScalar()
                        If TempStatusID = 0 Then
                            'Throw New Exception("Invalid return of TempStatusID after dbCmdWCond.ExecuteScalar() writing new Stop fields.")
                        End If

                    Next i
                End If
                Debug.WriteLine(vbCrLf)
            End If
            'Writes the updated status for the processed StatusID line into the list
            RecordCount = dbCmdWIDList.ExecuteNonQuery
            If RecordCount <> 1 Then
                Throw New Exception("Invalid RecordCount from initial dbDT_Orig datatable fill command.")
            End If
            'Clean up
            dbDA_Orig.Dispose()
            dbDA_Stops.Dispose()
            dbDA_IDs.Dispose()
            dbConnOld.Close()
            dbConnNew.Close()

        Catch ex As Exception
            MsgBox(ex.Message)
        End Try

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

    Sub DoAll()
        Dim sw As New Stopwatch
        Dim RecordCount, StopsCount As Long

        Dim dbDT_Orig, dbDT_Stops, dbDT_FilteredStops As DataTable
        Dim dbDA_Orig, dbDA_Stops As SqlDataAdapter
        Dim dbCmdRead, dbCmdRStops, dbCmdWStat, dbCmdWCond, dbCmdWStop, dbCmdWIDList As SqlCommand
        Dim dbConnOld, dbConnNew As SqlConnection
        Dim Record, StopRow As DataRow
        Dim strReadOrigSQL, strReadStopsSQL, strWriteStatusSQL, strWriteConditionSQL, strWriteStopsSQL, strWriteStatusIDConverted As String
        Dim strConnOld, strConnNew, strMsg, strKeyVal As String
        Dim NewStatusID, TempStatusID, n, i, KeyVal As Long
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

        'Creates Parameters for database writing
        'Dim Parameter Lists
        Dim SQLStatusParams As New List(Of SqlParameter)
        Dim SQLStopsReadParams As New List(Of SqlParameter)
        Dim SQLStopsParams As New List(Of SqlParameter)
        Dim SQLCondParams As New List(Of SqlParameter)

        'Status Values
        Dim Param100 As New SqlParameter("@Stamp", vbNull)
        Dim Param101 As New SqlParameter("@Comms", False)
        Dim Param102 As New SqlParameter("@WCID", vbNull)
        Dim Param103 As New SqlParameter("@Power", False)
        Dim Param104 As New SqlParameter("@ProdMode", False)
        Dim Param105 As New SqlParameter("@JobNumber", vbNull)
        Dim Param106 As New SqlParameter("@JobQty", vbNull)
        Dim Param107 As New SqlParameter("@CurrQty", vbNull)
        Dim Param108 As New SqlParameter("@Running", False)
        Dim Param109 As New SqlParameter("@MachFault", vbNull)
        Dim Param110 As New SqlParameter("@MachFaultAck", vbNull)
        Dim Param111 As New SqlParameter("@OpStop", vbNull)
        Dim Param112 As New SqlParameter("@Activity", vbNull)
        Dim Param113 As New SqlParameter("@Speed", vbNull)
        Dim Param114 As New SqlParameter("@ET", vbNull)
        SQLStatusParams.Add(Param100) : SQLStatusParams.Add(Param101) : SQLStatusParams.Add(Param102)
        SQLStatusParams.Add(Param103) : SQLStatusParams.Add(Param104) : SQLStatusParams.Add(Param105)
        SQLStatusParams.Add(Param106) : SQLStatusParams.Add(Param107) : SQLStatusParams.Add(Param108)
        SQLStatusParams.Add(Param109) : SQLStatusParams.Add(Param110) : SQLStatusParams.Add(Param111)
        SQLStatusParams.Add(Param112) : SQLStatusParams.Add(Param113) : SQLStatusParams.Add(Param114)

        'For use in STOPS read query
        Dim Param150 As New SqlParameter("@OrigStatusID", vbNull)
        SQLStopsReadParams.Add(Param150)

        'Condition Values
        Dim Param200 As New SqlParameter("@CondStatusID", vbNull)
        Dim Param201 As New SqlParameter("@TempMotor", vbNull)
        Dim Param202 As New SqlParameter("@TempGearBox", vbNull)
        Dim Param203 As New SqlParameter("@TempFeeder", vbNull)
        Dim Param204 As New SqlParameter("@TempIndexer", vbNull)
        SQLCondParams.Add(Param200) : SQLCondParams.Add(Param201) : SQLCondParams.Add(Param202)
        SQLCondParams.Add(Param203) : SQLCondParams.Add(Param204)

        'Stop Values
        Dim Param300 As New SqlParameter("@StopStatusID", vbNull)
        Dim Param301 As New SqlParameter("@MStop", False)
        Dim Param302 As New SqlParameter("@OStop", False)
        Dim Param303 As New SqlParameter("@MStopCode", vbNull)
        Dim Param304 As New SqlParameter("@OStopCode", vbNull)
        Dim Param305 As New SqlParameter("@StopCode", vbNull)
        SQLStopsParams.Add(Param300) : SQLStopsParams.Add(Param301) : SQLStopsParams.Add(Param302)
        SQLStopsParams.Add(Param303) : SQLStopsParams.Add(Param304) : SQLStopsParams.Add(Param305)

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
        strReadOrigSQL = "SELECT top (2000) * FROM [Status_RST-XVI] Where (Stamp >= CONVERT(DATETIME, '2019-04-01 00:00:00', 102)) AND (StatusID > " & KeyVal.ToString & ") ORDER BY STAMP ASC;"
        'Read all Stops for a given StatusID
        strReadStopsSQL = "SELECT * FROM [Stops] WHERE (StatusID = @OrigStatusID) " ' _
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

        'Assign Parameters to the Command Objects

        Console.WriteLine("Preparing Queries...")

        'Prepares objects for Stops processing later on
        dbDT_Stops = New DataTable
        dbDA_Stops = New SqlDataAdapter(dbCmdRStops)

        'Start by filling Datatable with Original Status_RST-XVI rows
        dbDT_Orig = New DataTable
        dbDA_Orig = New SqlDataAdapter(dbCmdRead)
        RecordCount = dbDA_Orig.Fill(dbDT_Orig)
        If RecordCount = 0 Then
            Throw New Exception("Invalid RecordCount from initial dbDT_Orig datatable fill command.")
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

        Dim intOrigStatusID, intNewStatusID As Long
        For Each row In dbDT_Orig.Rows
            Try
                'Make New Machine_Status entry
                intOrigStatusID = row("StatusID")
                intNewStatusID = AddStatusRow(row)

                If intNewStatusID = 0 Then
                    Throw New Exception("Exception in AddStatusRow on original StatusID: " & row("StatusID").ToString)
                End If
                'Make New EquipCond_RST-XVI entry
                If AddXVICondRow(row, intNewStatusID) = 0 Then
                    Throw New Exception("Exception in AddStatusRow on original StatusID: " & row("StatusID").ToString)
                End If

            Catch ex As Exception

            End Try

        Next


        Using bulkCopy As SqlBulkCopy = New SqlBulkCopy(dbConnNew.ToString, SqlBulkCopyOptions.KeepIdentity)
            bulkCopy.DestinationTableName = "dbo.Machine_Status"
            'SqlBulkCopyOptions.KeepIdentity = True
            Try
                bulkCopy.WriteToServer(tblNewStatus)
            Catch ex As Exception
                MsgBox(ex.Message)
            End Try
        End Using

        Console.Clear()
        Console.WriteLine("Current Status")
        Threading.Thread.Sleep(500)
        'Start stopwatch for timing purposes
        sw.Start()
        TSpan1 = TimeSpan.FromSeconds(0)





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
        Try
            newStopRow = tblNewStops.NewRow()
            newStopRow("StatusID") = NewID
            newStopRow("MStop") = appendRow("MStop")
            newStopRow("OStop") = appendRow("OStop")
            newStopRow("MStopCode") = appendRow("MStopCode")
            newStopRow("OStopCode") = appendRow("OStopCode")
            newStopRow("StopCode") = appendRow("StopCode")
            tblNewStops.Rows.Add(newStopRow)
            tblNewStops.AcceptChanges()
            AddNewStopsRow = newStopRow("ECID")
        Catch ex As Exception
            MsgBox(ex.Message)
            Beep()
        End Try

    End Function
End Module
