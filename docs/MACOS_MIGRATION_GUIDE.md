# macOS DB Server Migration Guide

n150 (Ubuntu 24.04) → macOS 마이그레이션 가이드.

---

## 1. PostgreSQL 설치

### Homebrew로 설치

```bash
# Homebrew 설치 (없는 경우)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# PostgreSQL 17 설치
brew install postgresql@17
```

### PATH 설정

```bash
# ~/.zshrc에 추가
echo 'export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### 서비스 시작

```bash
# 시작 (부팅 시 자동 시작 포함)
brew services start postgresql@17

# 상태 확인
brew services list
pg_isready
```

---

## 2. PostgreSQL 초기 설정

### 사용자 및 데이터베이스 생성

Homebrew PostgreSQL은 현재 macOS 사용자로 superuser가 자동 생성됩니다.

```bash
# DB 접속 확인
psql postgres

# 전용 사용자 생성 (psql 내에서)
CREATE USER sw_admin WITH PASSWORD 'your_password_here';
ALTER USER sw_admin CREATEDB;

# 데이터베이스 생성
CREATE DATABASE solar_images OWNER sw_admin;
CREATE DATABASE space_weather OWNER sw_admin;

# 권한 부여
GRANT ALL PRIVILEGES ON DATABASE solar_images TO sw_admin;
GRANT ALL PRIVILEGES ON DATABASE space_weather TO sw_admin;

\q
```

### 원격 접속 허용 (다른 머신에서 접속 필요한 경우)

데이터 디렉토리 위치 확인:

```bash
psql postgres -c "SHOW data_directory;"
# 일반적으로: /opt/homebrew/var/postgresql@17
```

**postgresql.conf** 수정:

```bash
# 파일 위치 확인 후 편집
vi /opt/homebrew/var/postgresql@17/postgresql.conf
```

```ini
# 모든 인터페이스에서 접속 허용 (기본값은 localhost만)
listen_addresses = '*'

# 포트 (기본값 유지)
port = 5432
```

**pg_hba.conf** 수정:

```bash
vi /opt/homebrew/var/postgresql@17/pg_hba.conf
```

```
# 파일 끝에 추가 — 내부 네트워크 대역에 맞게 수정
# TYPE  DATABASE        USER        ADDRESS           METHOD
host    all             all         192.168.0.0/24    scram-sha-256
host    all             all         10.0.0.0/8        scram-sha-256
```

설정 적용:

```bash
brew services restart postgresql@17
```

### 환경 변수 설정

```bash
# ~/.zshrc에 추가
export DB_HOST=localhost
export DB_USER=sw_admin
export DB_PASSWORD=your_password_here
```

---

## 3. NFS 마운트

### NFS 서버 정보 확인

NAS 서버에서 export된 경로를 확인합니다:

```bash
# NAS IP와 export 경로 확인 (NAS 서버에서)
showmount -e <NAS_IP>
```

### 마운트 포인트 생성

```bash
sudo mkdir -p /opt/nas/archive
```

### 수동 마운트 (테스트)

```bash
# NFSv3 (일반적인 NAS 호환성)
sudo mount -t nfs -o resvport,rw,soft,intr,timeo=30 <NAS_IP>:/volume1/archive /opt/nas/archive

# 마운트 확인
df -h /opt/nas/archive
ls /opt/nas/archive/
```

> **macOS 주의**: macOS NFS 클라이언트는 `resvport` 옵션이 필수입니다. 이 옵션 없이는 "Operation not permitted" 오류가 발생합니다.

### 자동 마운트 설정 (autofs)

macOS는 `/etc/auto_master` + `/etc/auto_nfs` 방식으로 자동 마운트를 설정합니다.

**방법 A: /etc/fstab (부팅 시 마운트)**

```bash
sudo vi /etc/fstab
```

```
# NAS_IP:/volume1/archive  /opt/nas/archive  nfs  resvport,rw,soft,intr,bg  0  0
<NAS_IP>:/volume1/archive  /opt/nas/archive  nfs  resvport,rw,soft,intr,bg  0  0
```

**방법 B: autofs (접근 시 자동 마운트, 권장)**

1. `/etc/auto_master` 편집:

```bash
sudo vi /etc/auto_master
```

```
# 마지막 줄에 추가
/opt/nas    auto_nas    -nosuid
```

2. `/etc/auto_nas` 생성:

```bash
sudo vi /etc/auto_nas
```

```
archive    -fstype=nfs,resvport,rw,soft,intr,bg    <NAS_IP>:/volume1/archive
```

3. autofs 재시작:

```bash
sudo automount -vc
```

4. 확인 (디렉토리 접근 시 자동 마운트):

```bash
ls /opt/nas/archive/
```

### NFS 마운트 문제 해결

| 증상 | 해결 |
|------|------|
| `Operation not permitted` | `resvport` 옵션 추가 |
| `Permission denied` | NAS에서 macOS IP에 대한 접근 권한 확인, NAS squash 설정 확인 |
| 마운트는 되지만 쓰기 불가 | NAS export 옵션에서 `rw` 확인, NAS 사용자 매핑(uid/gid) 확인 |
| 접속 매우 느림 | `nfsvers=3` 명시 또는 `nconnect=4` 추가 시도 |

---

## 4. 기존 데이터 마이그레이션

### 방법 A: 테이블/스키마만 이전 (데이터 재다운로드)

파일 데이터는 NFS에 있으므로 DB 스키마만 새로 생성하면 됩니다:

```bash
cd /Users/eunsupark/Projects/02_Harim/01_AP/setup-sw-db

# 테이블 생성
python scripts/create_all_tables.py

# 데이터 재다운로드
python scripts/download_omni.py --all --start 2010 --end 2025
python scripts/download_hpo.py --all --start 1985 --end 2025

# 30분 집계 테이블 빌드
python scripts/build_sw_30min.py build --start-year 2010 --end-year 2025

# 이미지 파일 DB 재등록 (NFS에 파일이 이미 있으므로)
python scripts/register_sdo.py /opt/nas/archive/sdo --parallel 8
python scripts/register_lasco.py --cameras c2 c3
python scripts/register_secchi.py --spacecrafts ahead behind --instruments cor1 cor2 euvi
```

### 방법 B: pg_dump로 DB 통째로 이전

n150에서 덤프:

```bash
# n150 (Ubuntu)에서 실행
pg_dump -h localhost -U sw_admin -Fc solar_images > solar_images.dump
pg_dump -h localhost -U sw_admin -Fc space_weather > space_weather.dump
```

macOS에서 복원:

```bash
pg_restore -h localhost -U sw_admin -d solar_images solar_images.dump
pg_restore -h localhost -U sw_admin -d space_weather space_weather.dump
```

---

## 5. 검증

```bash
# DB 접속 테스트
psql -h localhost -U sw_admin -d space_weather -c "SELECT COUNT(*) FROM sw_30min;"
psql -h localhost -U sw_admin -d solar_images -c "SELECT COUNT(*) FROM sdo;"

# NFS 파일 접근 테스트
ls /opt/nas/archive/sdo/aia/
ls /opt/nas/archive/space_weather/omni/

# 전체 파이프라인 테스트
cd /Users/eunsupark/Projects/02_Harim/01_AP/setup-sw-db
python scripts/download_hpo.py --all --nowcast
```

---

## 6. macOS 특이사항

- **sleep 방지**: DB 서버로 사용 시 macOS가 잠자기에 들어가지 않도록 설정
  ```bash
  # 시스템 설정 > 에너지 > "디스플레이가 꺼져 있을 때 자동으로 잠자기 방지" 체크
  # 또는 CLI:
  sudo pmset -a disablesleep 1
  sudo pmset -a sleep 0
  ```

- **방화벽**: PostgreSQL 포트 허용 (원격 접속 필요 시)
  ```
  시스템 설정 > 네트워크 > 방화벽 > 옵션 > postgres 허용
  ```

- **Homebrew 서비스 자동 시작**: `brew services start`로 등록하면 로그인 시 자동 시작됩니다. 시스템 부팅 시 시작하려면:
  ```bash
  sudo brew services start postgresql@17
  ```
