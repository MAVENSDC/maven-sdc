;+
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; mvn_orbproj_panel_brain.pro
;
; Procedure to create a 2D orbit projection plot for a MAVEN spaecraft
; trajectory. 
;
; Syntax:
;      trange = mvn_orbproj_panel_brain, trange, res, /xy
;
; Inputs:
;      trange            - 2-elelment timerange over which to plot the
;                          trajectory
;
;      res               - The time resolution to use when plotting
;
;      xy, xz, yz        - Boolean keywords indicating which
;                          projection to plot. No default.
;
;      solidmars         - Boolean: Treat Mars as a solid
;                          object (don't plot trajectory behind
;                          the planet). Default = not set
;
;      crustalfields     - Boolean: Color Mars with a map of the
;                          radial component of crustal magnetic field,
;                          as measured by MGS. Default = 1
;
;      showbehind        - Boolean: Even if Mars is solid, show the
;                          trajectory behind the planet (using
;                          different, smaller symbols). Default = not
;                          set
;
;      [xy]range         - 2-element bounds for the plot, in
;                          Rm. Default = [-3,3]
;
;      showperiapsis     - Boolean: Indicate the location of
;                          periapsis. Default = not set
;
;      showticks         - Indicate regular intervals (centered on
;                          periapsis) along the trajectory. Default =
;                          not set
;
;      tickinterval      - The interval at which to show ticks, in
;                          seconds. Default = 600
;
;      symsize           - Symbol size to use for the
;                          trajectory. Sizes of periapsis, interval
;                          ticks, crustal fields all scale from
;                          this. Default = 1.
;
;      charsize          - Character size to use for plot
;                          axes. Default = 1
;
;      noerase           - Boolean, default = 0. Setting to 1
;                          indicates that and plot currently on the
;                          plot device should not be erased.
;
; Dependencies:
;      1. Berkeley MAVEN software
;      2. ctload.pro - routine from David Fanning (coyote) to load
;                      color tables, including Brewer colors.
;      3. colorscale.pro - Dave Brain version of bytscl.pro
;      4. Map of crustal field Br - edit line ~177 to point to the
;                                   relevant location on your machine
;
; Comments:
;      1. This was developed for use with a single orbit
;         trajectory. I'm not sure how it would work with
;         something larger.
;      2. This routine acts on tplot variables loaded using
;         mvn_load_eph_brain.pro. If it doesn't find them, it
;         will load them.
;
; Dave Brain
; 12 January, 2017 -Initial version
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;-
pro mvn_orbproj_panel_brain, $
   trange=trange, res=res, $
   xy=xy, xz=xz, yz=yz, $
   solidmars = solidmars, crustalfields=crustalfields, $
   showbehind = showbehind, $
   xrange = xrange, yrange = yrange, $
   showperiapsis = showperiapsis, $
   showticks = showticks, tickinterval = tickinterval, $
   symsize = ss, charsize = cs, $
   noerase = noerase


;;; Parameters
  Rm = 3390.
  orbitcolortable = 63
  

;;; Get passed keywords
  if n_elements(trange) eq 0 then get_timespan, trange
  if n_elements(res) eq 0 then res = 60d0
  if n_elements(xrange) eq 0 then xrange = [-3,3]
  if n_elements(yrange) eq 0 then yrange = [-3,3]
  if keyword_set(crustalfields) eq 1 then solidmars = 1
  if n_elements(ss) eq 0 then ss = 1
  if n_elements(cs) eq 0 then cs = 1
  if n_elements(noerase) eq 0 then noerase = 0
  if n_elements(tickinterval) eq 0 then tickinterval = 600d0
  if keyword_set(showticks) then showperiapsis = 1


;;; Check for ephemeris at proper time resolution
;;;  and load it if it isn't already there and perfect
;;;  Along the way, get the S/C posn in MSO coordinates
  names = tnames()
  mso_present = where(names eq 'mvn_eph_mso')
  alt_present = where(names eq 'mvn_alt')
  lon_present = where(names eq 'mvn_lon')
  if mso_present[0] eq -1 or $
     alt_present[0] eq -1 or $
     lon_present[0] eq -1 then mvn_load_eph_brain, trange, res
  get_data, 'mvn_eph_mso', time, mso
  if time[1] - time[0] ne res or $
     time[0] gt trange[0] or $
     time[-1] lt trange[1] then begin
     mvn_load_eph_brain, trange, res
     get_data, 'mvn_eph_mso', time, mso
  endif
  mso /= Rm


;;; Get xdata and ydata for latitude lines, in MSO
  nlat = 5
  ndots = 720.
  xlat = fltarr(ndots, nlat)
  ylat = fltarr(ndots, nlat)
  zlat = fltarr(ndots, nlat)
  for i = 0, nlat-1 do begin
     curlat = ( 180./(nlat+1.) * (i+1) - 90. ) * !dtor
     xtmp = Rm * cos( findgen(ndots) * 360. / ndots * !dtor ) * $
            cos(curlat)
     ytmp = Rm * sin( findgen(ndots) * 360. / ndots * !dtor ) * $
            cos(curlat)
     ztmp = replicate(Rm*sin(curlat),ndots)
     ans = spice_vector_rotate( transpose([[xtmp],[ytmp],[ztmp]]), $
                                replicate(mean(trange),ndots), $
                                'IAU_MARS', $
                                'MAVEN_MSO' )
     xlat[*,i] = ans[0,*] / Rm
     ylat[*,i] = ans[1,*] / Rm
     zlat[*,i] = ans[2,*] / Rm
  endfor                        ; i
   
         
;;; Get xdata and ydata for longitude lines, in MSO
  nlon = 8.
  ndots = 720.
  xlon = fltarr(ndots, nlon)
  ylon = fltarr(ndots, nlon)
  zlon = fltarr(ndots, nlon)
  for i = 0, nlon-1 do begin
     curlon = 360. / nlon * i * !dtor
     xtmp = Rm * cos( findgen(ndots) * 360. / ndots * !dtor ) * $
            cos(curlon)
     ytmp = Rm * cos( findgen(ndots) * 360. / ndots * !dtor ) * $
            sin(curlon)
     ztmp = Rm * sin( findgen(ndots) * 360. / ndots * !dtor )
     ans = spice_vector_rotate( transpose([[xtmp],[ytmp],[ztmp]]), $
                                replicate(mean(trange),ndots), $
                                'IAU_MARS', $
                                'MAVEN_MSO' )
     xlon[*,i] = ans[0,*] / Rm
     ylon[*,i] = ans[1,*] / Rm
     zlon[*,i] = ans[2,*] / Rm
  endfor                        ; i


;;; Get the crustal fields
  if keyword_set(crustalfields) eq 1 then begin
     CD, current=c
     restore, c+'/br_360x180_pc.sav'
     mapcolor = colorscale( br, mindat=-70, maxdat=70, mincol=0, maxcol=255 )
     nummaplon = n_elements(br[*,0])
     nummaplat = n_elements(br[0,*])
     maplonres = 360./nummaplon
     maplatres = 180./nummaplat
     maplons = findgen(nummaplon) * maplonres + maplonres/2.
     maplats = findgen(nummaplat) * maplatres - 90. + maplonres/2.
     mapx = fltarr(nummaplon,nummaplat)
     mapy = mapx
     mapz = mapx
     periapse_time = mean(trange)
     ut = time_double(periapse_time)
     et = time_ephemeris(ut,/ut2et)
     qrot =  spice_body_att('IAU_MARS','MAVEN_MSO',ut,/quaternion)
     for i = 0, nummaplon-1 do begin
        for j = 0, nummaplat-1 do begin
           xtmp = Rm * cos(maplons[i]*!dtor) * cos(maplats[j]*!dtor)
           ytmp = Rm * sin(maplons[i]*!dtor) * cos(maplats[j]*!dtor)
           ztmp = Rm * sin(maplats[j]*!dtor)
           ans = spice_vector_rotate( [ xtmp, ytmp, ztmp ], $
                                      periapse_time, $
                                      et=et, $
                                      'IAU_MARS', $
                                      'MAVEN_MSO',$
                                      qrot=qrot)
           mapx[i,j] = ans[0] / Rm
           mapy[i,j] = ans[1] / Rm
           mapz[i,j] = ans[2] / Rm
        endfor                 
     endfor  
  endif


;;; Get periapsis location
  if keyword_set(showperiapsis) then begin
     peritimes = dindgen( trange[1] - trange[0] + 1 ) + trange[0]
     perixdat = spline( time, mso[0,*], peritimes )
     periydat = spline( time, mso[1,*], peritimes )
     perizdat = spline( time, mso[2,*], peritimes )
     perirad = sqrt( perixdat^2 + periydat^2 + perizdat^2 )
     minrad = min(perirad,periind)
     perixdat = perixdat[periind]
     periydat = periydat[periind]
     perizdat = perizdat[periind]
  endif

  
;;; Get locations of tick marks
  if keyword_set(showticks) then begin
     ;; Get times to show
     peritime = peritimes[periind]
     timeinterval = trange[1]-trange[0]
     ticktimes = [peritime]
     curtime = peritime - tickinterval
     while curtime gt trange[0] do begin
        ticktimes = [ curtime, ticktimes ]
        curtime -= tickinterval
     endwhile
     curtime = peritime + tickinterval
     while curtime lt trange[1] do begin
        ticktimes = [ ticktimes, curtime ]
        curtime += tickinterval
     endwhile
     tickxdat = spline( time, mso[0,*], ticktimes )
     tickydat = spline( time, mso[1,*], ticktimes )
     tickzdat = spline( time, mso[2,*], ticktimes )
  endif

  
;;; Make some choices depending upon the projection
  case 1 of
     keyword_set(xy): begin
        xtitle = 'MSO X  (R!DM!N)'
        ytitle = 'MSO Y  (R!DM!N)'
        cfxdat = mapx
        cfydat = mapy
        cfzdat = mapz
        latxdat = xlat
        latydat = ylat
        latzdat = zlat
        lonxdat = xlon
        lonydat = ylon
        lonzdat = zlon
        msoxdat = mso[0,*]
        msoydat = mso[1,*]
        msozdat = mso[2,*]
        if keyword_set(showperiapsis) then begin
           perix = perixdat
           periy = periydat
           periz = perizdat
        endif
        if keyword_set(showticks) then begin
           tickx = tickxdat
           ticky = tickydat
           tickz = tickzdat
        endif
     end
     keyword_set(xz): begin
        xtitle = 'MSO X  (R!DM!N)'
        ytitle = 'MSO Z  (R!DM!N)'
        cfxdat = mapx
        cfydat = mapz
        cfzdat = -1.*mapy
        latxdat = xlat
        latydat = zlat
        latzdat = -1.*ylat
        lonxdat = xlon
        lonydat = zlon
        lonzdat = -1.*ylon
        msoxdat = mso[0,*]
        msoydat = mso[2,*]
        msozdat = -1.*mso[1,*]
        if keyword_set(showperiapsis) then begin
           perix = perixdat
           periy = perizdat
           periz = -1.*periydat
        endif
        if keyword_set(showticks) then begin
           tickx = tickxdat
           ticky = tickzdat
           tickz = -1.*tickydat
        endif
     end
     keyword_set(yz): begin
        xtitle = 'MSO Y  (R!DM!N)'
        ytitle = 'MSO Z  (R!DM!N)'
        cfxdat = mapy
        cfydat = mapz
        cfzdat = mapx
        latxdat = ylat
        latydat = zlat
        latzdat = xlat
        lonxdat = ylon
        lonydat = zlon
        lonzdat = xlon
        msoxdat = mso[1,*]
        msoydat = mso[2,*]
        msozdat = mso[0,*]
        if keyword_set(showperiapsis) then begin
           perix = periydat
           periy = perizdat
           periz = perixdat
        endif
        if keyword_set(showticks) then begin
           tickx = tickydat
           ticky = tickzdat
           tickz = tickxdat
        endif
     end     
  endcase
  










  

  
  
  
;;; Set up for the plot
   !p.background = 255
   !p.color = 0

;;; Make the plot axes
   ctload, 0
   plot, [0], [0], /nodata, /iso, $
         xrange = xrange, xstyle = 1, $
         xtitle = xtitle, ytitle = ytitle, $
         yrange = yrange, ystyle = 1, $
         xthick = 2, ythick = 2, $
         charsize = cs, noerase = noerase

   
;;; Fill the plot area with gray
   polyfill, color=220, $
             [ xrange, reverse(xrange) ], $
             [ [1,1]*yrange[0], [1,1]*yrange[1] ]


;;; Add the crustal fields
   if keyword_set(crustalfields) then begin
      xdat = reform(cfxdat,nummaplon*nummaplat)
      ydat = reform(cfydat,nummaplon*nummaplat)
      zdat = reform(cfzdat,nummaplon*nummaplat)
      color = reform(mapcolor,nummaplon*nummaplat)
      ord = sort(zdat)
      xdat = xdat[ord]
      ydat = ydat[ord]
      zdat = zdat[ord]
      color = color[ord]
      keep = where(zdat ge 0, keepcnt)
      if keepcnt gt 0 then begin
         ctload, 22, /brewer, /reverse
         plots, xdat[keep], ydat[keep], color=color[keep], $
                psym=symcat(14), symsize=ss*.25
         ctload, 0
      endif
   endif
      

;;; Draw and shade Mars
   ang = findgen(361) * !dtor
   if keyword_set(xy) or keyword_set(xz) then $
      polyfill, [0,cos(ang[91:271]),0], [0,sin(ang[91:271]),0], $
                color=135, /line_fill, orientation=45, spacing=0.1
   oplot, cos(ang), sin(ang), thick=2
   

;;; Show the latitude lines
   keep = where( latzdat ge 0, keepcnt )
   if keepcnt gt 0 then plots, latxdat[keep], latydat[keep], psym=3


;;; Show the longitude lines
   keep = where( lonzdat ge 0, keepcnt )
   if keepcnt gt 0 then plots, lonxdat[keep], lonydat[keep], psym=3


;;; Colorscale the trajectory
   color = colorscale( time, $
                       mindat=min(trange), maxdat=max(trange), $
                       mincol=20, maxcol=250 )

   
;;; Plot the trajectory
   ctload, 63
   cdat = color
   ;; Sort it by distance from observer
   ord = sort(msozdat)
   xdat = msoxdat[ord]
   ydat = msoydat[ord]
   zdat = msozdat[ord]
   cdat = cdat[ord]
   ;; If Mars is solid we either have to get rid
   ;; or some data or display it differently
   keepcnt = 1
   if keyword_set(solidmars) then begin
      keep = where( zdat gt 0 or $
                    sqrt(xdat^2. + ydat^2.) gt 1, keepcnt, $
                    complement = keep2, ncomp = keep2cnt )
      ;; Display the trajectory half size behind Mars if requested
      if keyword_set(showbehind) eq 1 and keep2cnt gt 0 then begin
         x2dat = xdat[keep2]
         y2dat = ydat[keep2]
         z2dat = zdat[keep2]
         c2dat = cdat[keep2]
         plots, x2dat, y2dat, psym=4, color=c2dat, symsize=ss/2.
      endif
      ;; Trim the trajectory to what's visible
      if keepcnt gt 0 then begin
         xdat = xdat[keep]
         ydat = ydat[keep]
         zdat = zdat[keep]
         cdat = cdat[keep]
      endif
   endif
   ;; Display the trajectory
   if keepcnt gt 0 then begin
      plots, xdat, ydat, $
             psym=symcat(14), color=cdat, symsize=ss
   endif

   
;;; Show ticks as black diamonds
   ctload, 0
   ;; Sort by distance from observer
   ord = sort(tickz)
   xdat = tickx[ord]
   ydat = ticky[ord]
   zdat = tickz[ord]
   ;; If Mars is solid we either have to get rid
   ;; or some data or display it differently
   keepcnt = 1
   if keyword_set(solidmars) then begin
      keep = where( zdat gt 0 or $
                    sqrt(xdat^2. + ydat^2.) gt 1, keepcnt, $
                    complement = keep2, ncomp = keep2cnt )
      ;; Display the trajectory half size behind Mars if requested
      if keyword_set(showbehind) eq 1 and keep2cnt gt 0 then begin
         x2dat = xdat[keep2]
         y2dat = ydat[keep2]
         z2dat = zdat[keep2]
         plots, x2dat, y2dat, psym=4, symsize=ss/2.
      endif
      ;; Trim the trajectory to what's visible
      if keepcnt gt 0 then begin
         xdat = xdat[keep]
         ydat = ydat[keep]
         zdat = zdat[keep]
      endif
   endif
   ;; Display the ticks
   if keepcnt gt 0 then begin
      plots, xdat, ydat, psym=symcat(14), symsize=ss/2.
   endif
   
   
;;; Show periapsis as black cross
   if keyword_set(showperiapsis) then begin
      vis = periz gt 0 or sqrt( perix^2 + periy^2 ) gt 1
      if vis then $
         plots, perix, periy, psym = symcat(34), symsize=ss*1.5
      if vis eq 0 and keyword_set(showbehind) eq 1 then $
         plots, perix, periy, psym = symcat(34), symsize=ss
   endif
   
   
;;; Redraw the axes
   plot, [0], [0], /nodata, /iso, $
         xrange = xrange, xstyle = 1, $
         xtitle = xtitle, ytitle = ytitle, $
         yrange = yrange, ystyle = 1, $
         xthick = 2, ythick = 2, $
         charsize = cs, /noerase
  


;;; Clean up
   cleanup

    

end
