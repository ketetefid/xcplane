Name:           xcplane
Version:        @VERSION@
Release:        1%{?dist}
Summary:        Infrastructure control plane for a privacy-first server fleet
License:        GPLv3+
URL:            https://github.com/ketetefid/xcplane
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  rust
Requires:       glibc sqlite-libs

%description
Infrastructure control plane for a privacy-first server fleet

%prep
%autosetup

%build
cargo build --release

%install
install -D target/release/xcplane %{buildroot}/usr/bin/xcplane

%files
/usr/bin/xcplane

%changelog
* Sat Aug 22 2026 Kete Tefid <ketetefid@gmail.com> - @VERSION@-1
- First public release of xcplane
- Quickstart video
- The base of documentation
