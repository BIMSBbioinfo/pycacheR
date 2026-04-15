;; Guix package definition for pycacheR (Python cachepy)
;; Install with: guix install -f guix.scm
;; Or use in a manifest: (specifications->manifest '("python-pycacher"))

(define-module (pycacher)
  #:use-module (guix packages)
  #:use-module (guix git-download)
  #:use-module (guix build-system pyproject)
  #:use-module ((guix licenses) #:prefix license:)
  #:use-module (gnu packages python-build)
  #:use-module (gnu packages python-check)
  #:use-module (gnu packages check))

(define-public python-pycacher
  (package
    (name "python-pycacher")
    (version "1.0.0")
    (source
     (origin
       (method git-fetch)
       (uri (git-reference
             (url "https://github.com/BIMSBbioinfo/pycacheR")
             (commit (string-append "v" version))))
       (file-name (git-file-name name version))
       (sha256
        (base32 "0sybpkja66yc5iy881lcxm7sri5ywqp3xc74d6cpmfxjijc4lv2x"))))
    (build-system pyproject-build-system)
    (arguments
     '(#:tests? #f))
    (native-inputs
     (list python-setuptools python-wheel python-pytest))
    (home-page "https://github.com/BIMSBbioinfo/pycacheR")
    (synopsis "Disk-backed memoization decorator with automatic dependency tracking")
    (description
     "pycacheR (imported as @code{cachepy}) is a disk-backed caching
decorator for Python that automatically detects changes in code,
arguments, and input files.  It caches function results to disk as
pickle files and tracks the full dependency graph.  Python port of
the R package cacheR.")
    (license license:gpl3+)))

python-pycacher
