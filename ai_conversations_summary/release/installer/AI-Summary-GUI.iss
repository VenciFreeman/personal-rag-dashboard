; Inno Setup script for AI Conversations Summary installer
; Build input expected at release\installer\staging\

[Setup]
AppId={{F3E31C38-6A3B-4D8C-9F7E-9B1E3F17C2A1}
AppName=AI Conversations Summary
AppVersion=1.0.0
AppPublisher=AI Conversations Summary
DefaultDirName={userappdata}\AI-Conversations-Summary
DefaultGroupName=AI Conversations Summary
DisableProgramGroupPage=yes
PrivilegesRequired=lowest
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
OutputDir=..\dist_installer
OutputBaseFilename=AI-Conversations-Summary-Setup
ArchitecturesInstallIn64BitMode=x64
UninstallDisplayIcon={app}\python\pythonw.exe

[Languages]
Name: "chinesesimp"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; GroupDescription: "Additional icons:"; Flags: unchecked

[Files]
Source: "staging\app\*"; DestDir: "{app}\app"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "staging\python\*"; DestDir: "{app}\python"; Flags: ignoreversion recursesubdirs createallsubdirs

[Dirs]
DestDir: "{app}\app\data"
DestDir: "{app}\app\data\raw_dir"
DestDir: "{app}\app\data\extracted_dir"
DestDir: "{app}\app\data\summarize_dir"
DestDir: "{app}\app\data\vector_db"
DestDir: "{app}\app\data\rag_sessions"
DestDir: "{app}\app\data\local_models"
DestDir: "{app}\app\documents"

[Icons]
Name: "{autoprograms}\AI Conversations Summary"; Filename: "{app}\python\pythonw.exe"; Parameters: "\"{app}\app\launch_gui.py\""; WorkingDir: "{app}\app"
Name: "{autodesktop}\AI Conversations Summary"; Filename: "{app}\python\pythonw.exe"; Parameters: "\"{app}\app\launch_gui.py\""; WorkingDir: "{app}\app"; Tasks: desktopicon

[Run]
Filename: "{app}\python\pythonw.exe"; Parameters: "\"{app}\app\launch_gui.py\""; WorkingDir: "{app}\app"; Description: "Launch AI Conversations Summary"; Flags: nowait postinstall skipifsilent
