;+
; NAME:
;       CTLOAD
;
; PURPOSE:
;
;       This is a drop-in replacement for the ITTVIS-supplied program LOADCT.
;       The same keywords used with LOADCT apply. In addition, a REVERSE keyword
;       is supplied to reverse the color table vectors, and a CLIP keyword is
;       supplied to be able to clip the normal LOADCT color table. This is
;       extremely useful if you wish to use a reduced number of colors. Also,
;       all color table loading is handled silently. (To fix a major pet-peeve
;       of mine.)
;
; AUTHOR:
;
;       FANNING SOFTWARE CONSULTING
;       David Fanning, Ph.D.
;       1645 Sheely Drive
;       Fort Collins, CO 80526 USA
;       Phone: 970-221-0438
;       E-mail: davidf@dfanning.com
;       Coyote's Guide to IDL Programming: http://www.dfanning.com
;
; CATEGORY:

;       Utilities
;
; CALLING SEQUENCE:
;
;       CTLOAD, table
;
; AUGUMENTS:
;
;       table:         Optional table number to load. Integer from 0 to the number of
;                      tables in the file, minus 1. Default value is 0.
;
; KEYWORDS:
;
;       BOTTOM:        The first color table index. Set to 0 by default.
;
;       BREWER:        Set this keyword if you wish to use the Brewer Colors, as
;                      implemented by Mike Galloy in the file brewer.tbl, and implemented
;                      here as fsc_brewer.tbl. See these references:
;
;                      Brewer Colors: http://www.personal.psu.edu/cab38/ColorBrewer/ColorBrewer_intro.html
;                      Mike Galloy Implementation: http://michaelgalloy.com/2007/10/30/colorbrewer.html
;
;                      This program will look first in the $IDL_DIR/resource/colors directory for 
;                      the color table file, and failing to find it there will look in the same 
;                      directory that the source code of this program is located, then in the IDL path. 
;                      Finally, if it still can't find the file, it will ask you to locate it.
;                      If you can't find it, the program will simply return without loading a color table.
;
;                      NOTE: YOU WILL HAVE TO DOWNLOAD THE FSC_BREWER.TBL FILE FROM THE COYOTE LIBRARY AND
;                      PLACE IT IN ONE OF THE THREE PLACES OUTLINED ABOVE:
;
;                      http://www.dfanning.com/programs/fsc_brewer.tbl
;
;       CLIP:          A one- or two-element integer array that indicates how to clip
;                      the original color table vectors. This is useful if you are
;                      restricting the number of colors, and do not which to have
;                      black or white (the usual color table end members) in the
;                      loaded color table. CLIP[0] is the lower bound. (A scalar
;                      value of CLIP is treated as CLIP[0].) CLIP[1] is the upper
;                      bound. For example, to load a blue-temperature color bar
;                      with only blue colors, you might type this:
;
;                        IDL> CTLOAD, 1, CLIP=[110,240]
;                        IDL> CINDEX
;
;                     Or, alternatively, if you wanted to include white at the upper
;                     end of the color table:
;
;                        IDL> CTLOAD, 1, CLIP=110
;                        IDL> CINDEX
;
;       RGB_TABLE:    If this keyword is set to a named variable, the color table
;                     is returned as an [NCOLORS,3] array and no colors are loaded
;                     in the display.
;
;       FILE:         The name of a color table file to open. By default colors1.tbl in
;                     the IDL directory.
;
;       GET_NAMES:    If set to a named variable, the names of the color tables are returned
;                     and no colors are loaded in the display. Note that RGB_TABLE cannot be
;                     used concurrently with GET_NAMES. Use two separate calls if you want both.
;
;       NCOLORS:      The number of colors loaded. By default, !D.TABLE_SIZE.
;
;       REVERSE:      If this keyword is set, the color table vectors are reversed.
;
;       SILENT:       This keyword is provided ONLY for compatibility with LOADCT. *All*
;                     color table manipulations are handled silently.
;
; EXAMPLES:
;
;       Suppose you wanted to create a color table that displayed negative values with
;       red-temperature values and positive values with blue-temperature values, and you
;       would like the red-temperature values to be reversed in the color table (so dark
;       colors adjoin in the color table and indicate values near zero). You could do this:
;
;           CTLoad, 0
;           CTLoad, 3, /REVERSE, CLIP=[32,240], BOTTOM=1, NCOLORS=10
;           CTLoad, 1, CLIP=[64, 245], BOTTOM=11, NCOLORS=10
;           Colorbar, NCOLORS=20, BOTTOM=1, DIV=10, RANGE=[-10,10]
;
;       Here is an example that shows the difference between LOADCT and CTLOAD:
;
;           ERASE, COLOR=FSC_COLOR('Charcoal)
;           LoadCT, 5, NCOLORS=8
;           Colorbar, NCOLORS=8, DIVISIONS=8, POSITION=[0.1, 0.65, 0.9, 0.75], XMINOR=0, XTICKLEN=1
;           CTLoad, 5, NCOLORS=8, CLIP=[16, 240]
;           Colorbar, NCOLORS=8, DIVISIONS=8, POSITION=[0.1, 0.35, 0.9, 0.45], XMINOR=0, XTICKLEN=1
;
; MODIFICATION HISTORY:
;
;       Written by David W. Fanning, 30 October 2007.
;       Added ability to read Brewer Color Table file, if available, with BREWER keyword. 14 May 2008. DWF.
;       Small change in the way the program looks for the Brewer file. 8 July 2008. DWF.
;       Changed the way the program looks for the Brewer color table file. Now use
;          the Coyote Library routine FIND_RESOURCE_FILE to look for the file. 29 June 2010. DWF. 
;-
;******************************************************************************************;
;  Copyright (c) 2008, by Fanning Software Consulting, Inc.                                ;
;  All rights reserved.                                                                    ;
;                                                                                          ;
;  Redistribution and use in source and binary forms, with or without                      ;
;  modification, are permitted provided that the following conditions are met:             ;
;                                                                                          ;
;      * Redistributions of source code must retain the above copyright                    ;
;        notice, this list of conditions and the following disclaimer.                     ;
;      * Redistributions in binary form must reproduce the above copyright                 ;
;        notice, this list of conditions and the following disclaimer in the               ;
;        documentation and/or other materials provided with the distribution.              ;
;      * Neither the name of Fanning Software Consulting, Inc. nor the names of its        ;
;        contributors may be used to endorse or promote products derived from this         ;
;        software without specific prior written permission.                               ;
;                                                                                          ;
;  THIS SOFTWARE IS PROVIDED BY FANNING SOFTWARE CONSULTING, INC. ''AS IS'' AND ANY        ;
;  EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES    ;
;  OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT     ;
;  SHALL FANNING SOFTWARE CONSULTING, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,             ;
;  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED    ;
;  TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;         ;
;  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND             ;
;  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT              ;
;  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS           ;
;  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.                            ;
;******************************************************************************************;
PRO CTLOAD, table, $
   BREWER=brewer, $
   BOTTOM=bottom, $
   CLIP = clip, $
   RGB_TABLE=color_table, $
   FILE=file, $
   GET_NAMES=get_names, $
   NCOLORS=ncolors, $
   REVERSE=reverse, $
   SILENT=silent, $
   ADDWHITE=addwhite, $
   ADDBLACK=addblack


   COMMON colors, r_orig, g_orig, b_orig, r_curr, g_curr, b_curr
   Compile_Opt idl2

   ; Error handling.
   Catch, theError
   IF theError NE 0 THEN BEGIN
      Catch, /CANCEL
      Help, LAST_MESSAGE=1, OUTPUT=traceback
      Help, Calls=callStack
      callingRoutine = (StrSplit(StrCompress(callStack[1])," ", /Extract))[0]
      Print,''
      Print, 'Traceback Report from ' + StrUpCase(callingRoutine) + ':'
      Print, ''
      FOR j=0,N_Elements(traceback)-1 DO Print, "     " + traceback[j]
      void = Dialog_Message(traceback[0], /Error, TITLE='Trapped Error')
      IF N_Elements(lun) NE 0 THEN Free_Lun, lun
      RETURN
   ENDIF

   ; Check keywords and arguments.
   IF N_Elements(table) EQ 0 THEN table = 0
   IF N_Elements(bottom) EQ 0 THEN bottom = 0 ELSE bottom = 0 > bottom < (!D.TABLE_SIZE-1)
   IF N_Elements(clip) EQ 0 THEN clip = [0,255]
   IF N_Elements(clip) EQ 1 THEN clip = [clip, 255]
   clip = 0 > clip < 255
   IF N_Elements(file) EQ 0 THEN file = Filepath('colors1.tbl', SUBDIRECTORY=['resource', 'colors'])
   
   ; Try to locate the brewer file. 
   IF Keyword_Set(brewer) THEN BEGIN
       brewerfile = Find_Resource_File('fsc_brewer.tbl')
       IF brewerfile EQ "" THEN BEGIN
            Message, 'Cannot find the Brewer color table file "fsc_brewer.tbl."' + $
                     ' Using normal IDL color tables.', /INFORMATIONAL
       ENDIF ELSE file = brewerfile
   ENDIF

   ; Be sure !D.TABLE_SIZE is established.
   IF (!D.NAME EQ 'X') AND (!D.WINDOW EQ -1) THEN BEGIN
      Window, /Free, /Pixmap, XSIZE=10, YSIZE=10
      WDelete, !D.WINDOW
   ENDIF

   IF N_Elements(ncolors) EQ 0 THEN ncolors = !D.TABLE_SIZE
   reverse = KEYWORD_SET(reverse)

   ; Open and read the color table files.
   OPENR, lun, file, /GET_LUN
   ntables = 0B
   READU, lun, ntables

   ; Make sure table number is within range.
   IF (table GE ntables) OR (table LT 0) THEN $
      Message, 'Table number must be from 0 to ' + StrTrim(Fix(ntables)-1,2) + '.'

   ; Read the table names, if required, and return.
   IF Arg_Present(get_names) THEN BEGIN
      get_names = BytArr(32, ntables)
      Point_LUN, lun, ntables * 768L + 1
      READU, lun, get_names
      FREE_LUN, LUN
      get_names = StrTrim(get_names, 2)
      RETURN
   ENDIF

   ; Read the color table.
   theTables = Assoc(lun, BytArr(256), 1)
   r = theTables[table*3]
   g = theTables[table*3+1]
   b = theTables[table*3+2]

   ; Close the file.
   FREE_LUN, lun

   ; Clip the colors.
   r = r[clip[0]:clip[1]]
   g = g[clip[0]:clip[1]]
   b = b[clip[0]:clip[1]]
   nclipcolors = (clip[1]-clip[0]) + 1

   ; Interpolate to the number of colors asked for.
   IF ncolors NE nclipcolors THEN BEGIN
      p = (Lindgen(ncolors) * nclipcolors) / (ncolors-1)
      r = r[p]
      g = g[p]
      b = b[p]
   ENDIF

  ; Need to reverse the colors?
  IF reverse THEN BEGIN
     r = Reverse(r)
     g = Reverse(g)
     b = Reverse(b)
  ENDIF

  ; Need to add white as color 255?
  IF keyword_set(addwhite) THEN BEGIN
     r[255] = 255
     g[255] = 255
     b[255] = 255
  ENDIF

  ; Need to add black as color 0?
  IF keyword_set(addblack) THEN BEGIN
     r[0] = 0
     g[0] = 0
     b[0] = 0
  ENDIF

  ; Load a color_table, if needed. Otherwise, load color vectors.
  IF Arg_Present(color_table) THEN BEGIN
     color_table = [[r], [g], [b]]
  ENDIF ELSE BEGIN
     r_orig = BYTSCL(Indgen(!D.TABLE_SIZE))
     g_orig = r_orig
     b_orig = r_orig
     r_orig[bottom] = r
     g_orig[bottom] = g
     b_orig[bottom] = b
     r_curr = r_orig
     g_curr = g_orig
     b_curr = b_orig
     TVLCT, r, g, b, bottom
  ENDELSE

END
