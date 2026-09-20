# Sky Control native.go

Do **not** download the short `native.go` in this repo. Join the five parts:

- native_a.go
- native_a2.go
- native_b1.go
- native_b2.go
- native_b3.go

## PowerShell (copy all of this)

```
cd $env:USERPROFILE\Downloads
$base = "https://raw.githubusercontent.com/SaintGenius/skycontrol-native-drop/main"
Invoke-WebRequest "$base/native_a.go"  -OutFile native_a.go
Invoke-WebRequest "$base/native_a2.go" -OutFile native_a2.go
Invoke-WebRequest "$base/native_b1.go" -OutFile native_b1.go
Invoke-WebRequest "$base/native_b2.go" -OutFile native_b2.go
Invoke-WebRequest "$base/native_b3.go" -OutFile native_b3.go
Get-Content native_a.go, native_a2.go, native_b1.go, native_b2.go, native_b3.go | Set-Content native.go
copy /Y native.go C:\Users\Rob\Downloads\skycontrol\skycontrol\internal\radio\native.go
```

Leave `native_rx.go` in that folder. Run BUILD.bat.
