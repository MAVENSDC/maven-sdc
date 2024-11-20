;+
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; mvn_orbit_survey_plot_ephemeris
;
; Routine to create an ephemeris survey plot with 6 different panels,
;  given a single timestamp. The plot is for the entire orbit
;  containing that timestamp
;
; Syntax: mvn_orbit_survey_plot_ephemeris, time, fileroot, screen=screen
;
; Inputs:
;      time              - any UTC timestamp, as string or double precision
;
;      fileroot          - the name of the file, without extension
;                          (i.e. no '.jpg' at the end)
;
;      screen            - boolean keyword to send the output to the
;                          screen; used for testing. Default = 0
;
; Dependencies:
;      There are many code dependencies here, not including the
;      obvious dependency on Berkeley's MAVEN software. I haven't had
;      time to compile a complete list, but here's a partial
;      list of code by Dave Brain that is needed to run this routine:
;      mvn_get_orbit_times_brain.pro - gets start/stop times for orbit
;      mvn_load_eph_brain.pro - loads ephemeris info into tplot vars
;      mvn_orbproj_panel_brain.pro - makes a projection plot
;      mvn_cylplot_panel_brain.pro - makes a cylindrical coords plot
;      mvn_groundtrack_panel_brain.pro - makes a groundtrack
;      mvn_threed_projection_panel_brain.pro - makes a 3D projection
;      ps_set.pro - establishes a postscript plot
;      plotposn.pro - positions a plot
;      cleanup.pro - resets plotting system variables
;      colorscale.pro - Dave Brain version of bytscl.pro
;      ctload.pro - Dave Fanning (coyote) version of loadct, with
;                   brewer color mode enabled.
;
; Caution:
;   1. VERY IMPORTANT! Three of the routines contain a hard link to a
;      file containing crustal field information. This link needs to be
;      edited for the location of the file on your machine.
;      Please check header comments for:
;         mvn_orbproj_panel_brain.pro
;         mvn_groundtrack_panel_brain.pro
;         mvn_threed_projection_panel_brain.pro
;   2. This routine makes use of the ImageMagick "convert" utility,
;      installed on Dave's machine. This step is used when
;      converting the .eps file producded by the program into a .jpg
;      file. If you don't have "convert", simply comment out
;      the lines below that create the .jpg and remove the .eps
;      files. (lines ~219-223, right under ;;;Cleanup)
;   3. This is optimized to work on Dave's machine. No
;      guarantees about your machine!
;
; Dave Brain
; 18 Jan, 2017 - Original version
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;-
pro mvn_orbit_survey_plot_ephemeris, time, fileroot

  ;;Setting numbers
  resolution = [2340, 936]*2
  projection_panel_size = [20,20]*2
  projection_panel_xrange = [-3,3]
  projection_panel_yrange = [-3,3]
  cylindrical_panel_size = [ 30, 15 ]*2
  cylindrical_xrange = [-3,3]
  cylindrical_yrange = [0,3]
  symbol_size = 1.0*2
  character_size = 1.5*2
  ground_track_panel_size = [ 30, 15 ]*2
  threed_panel_size = [ 26, 26 ]*2
  threed_xrange = [-1,1]*1.2
  threed_yrange = [-1,1]*1.2
  
  
  thisDevice = !D.Name
  
;;; Find the start and stop time of the orbit
  t0 = time_double( time )
  trange = mvn_get_orbit_times_brain( t0 )

  ;Set the resolution so that the screen is about 45x18 cm, may require tweaking on other systems
  set_plot, 'Z'
  device, set_resolution=resolution, Decomposed=0, Set_Pixel_Depth=24
  !P.Color = '000000'xL
  !P.Background = 'FFFFFF'xL
  !P.Font = 1
  erase
  
  
  
;;; Get time of periapsis
  mvn_load_eph_brain, trange, 1d0
  get_data, 'mvn_alt', t, alt
  ans = min(alt, ind)
  peri_time = t[ind]


  
;;; Create the three projection plots
  ans = plotposn( size=projection_panel_size, cen=[ 10, 25.5 ]*2, /region )
  mvn_orbproj_panel_brain, $
     /xy, trange=trange, res=60d0, $
     /crustal, /showbehind, $
     xrange = projection_panel_zrange, yrange = projection_panel_yrange, $
     symsize = symbol_size, charsize = character_size, $
     /showperiapsis, /showticks, /noerase
     
  ans = plotposn( size=projection_panel_size, cen=[ 30, 25.5 ]*2, /region )
  mvn_orbproj_panel_brain, $
     /xz, trange=trange, res=60d0, $
     /crustal, /showbehind, $
     xrange = projection_panel_zrange, yrange = projection_panel_yrange, $
     symsize = symbol_size, charsize = character_size, $
     /showperiapsis, /showticks, /noerase

  ans = plotposn( size=projection_panel_size, cen=[ 50, 25.5 ]*2, /region )
  mvn_orbproj_panel_brain, $
     /yz, trange=trange, res=60d0, $
     /crustal, /showbehind, $
     xrange = projection_panel_zrange, yrange = projection_panel_yrange, $
     symsize = symbol_size, charsize = character_size, $
     /showperiapsis, /showticks, /noerase

  

;;; Create the cylindrical coordinates plot
  ans = plotposn( size=cylindrical_panel_size, cen=[ 15, 7.5 ]*2, /region )
  mvn_cylplot_panel_brain, $
     trange = trange, res = 60d0, $
     xrange = cylindrical_xrange, yrange= cylindrical_yrange, $
     /showperiapsis, /showticks, /noerase, $
     symsize = symbol_size, charsize = character_size

;;; Create the groundtrack plot
  ans = plotposn( size=ground_track_panel_size, cen=[ 45 , 7.5 ]*2, /region )
  mvn_groundtrack_panel_brain, $
     trange = trange, res = 60d0, $
     /terminator, /noerase, $
     /showperiapsis, /showticks, $
     symsize = symbol_size, charsize = character_size


;;; Create the projection centered on periapsis
  ans = plotposn( size=threed_panel_size, cen=[ 76, 21 ]*2, /region )
  mvn_threed_projection_panel_brain, $
     trange=trange, res=10d0, $
     /noerase, $
     xrange = threed_xrange, yrange = threed_yrange, $
     /showperiapsis, /showticks, $
     symsize = symbol_size*1.2
  
  
;;; Create the timebar
  color = colorscale( t, mindat=min(trange), maxdat=max(trange), $
                      mincol=20, maxcol=250 )
  store_data, $
     'colorbar', $
     data={ x:t, $
            y:[[color],[color]], $
            v:[0,1] }
  
  options, 'colorbar', 'ytitle', ' '
  options, 'colorbar', 'yticks', 1
  options, 'colorbar', 'yminor', 1
  options, 'colorbar', 'ytickname', [' ',' ']
  options, 'colorbar', 'spec', 1
  options, 'colorbar', 'no_color_scale', 1
  
  time_stamp, /off
  tplot_options, 'xmargin', [10,1]
  tplot_options, 'ymargin', [1,1]
  tplot_options, 'noerase', 1
  tplot_options, 'charsize', 1.1*2
  tplot_options, 'region', [0.675,0.1,0.975,0.18]
  ctload, 63, rgb=rgb
  rgb[255,*] = 255
  rgb[0,*] = 0
  tvlct, rgb[*,0], rgb[*,1], rgb[*,2]
  tplot, 'colorbar', trange = trange


;;; Make title
  xyouts, /normal, 0.5, 0.93, charsize=2.5*2, charthick=4, al=0.5, $
          time_string(peri_time)
          
          
  image24 = TVRD(TRUE=1)
  image24 = rebin(image24, 3, 2340, 936)
  Write_JPEG, fileroot+ '.jpg', image24, True=1, Quality=100
  cleanup

end
