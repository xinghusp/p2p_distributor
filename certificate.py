"""自签名证书生成模块"""
import datetime
import logging
import os
from typing import Dict, Tuple

from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

from config import CERT_FILE, KEY_FILE, CERT_DIR

def generate_self_signed_cert() -> Tuple[str, str]:
    """
    生成自签名证书
    
    返回:
        Tuple[str, str]: 证书文件路径和私钥文件路径的元组
    """
    # 确保证书目录存在
    os.makedirs(CERT_DIR, exist_ok=True)
    
    # 如果证书和私钥已存在，直接返回
    if os.path.exists(CERT_FILE) and os.path.exists(KEY_FILE):
        logging.info(f"证书已存在: {CERT_FILE}")
        return str(CERT_FILE), str(KEY_FILE)
    
    logging.info("生成新的自签名证书...")
    
    # 生成私钥
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    
    # 准备证书主题和颁发者信息
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, u"CN"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"Classroom"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, u"LocalNetwork"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"P2P File Distributor"),
        x509.NameAttribute(NameOID.COMMON_NAME, u"classroom.local"),
    ])
    
    # 准备证书
    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        private_key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.datetime.utcnow()
    ).not_valid_after(
        # 有效期1年
        datetime.datetime.utcnow() + datetime.timedelta(days=365)
    ).add_extension(
        x509.SubjectAlternativeName([
            x509.DNSName("localhost"),
            x509.DNSName("classroom.local"),
            # 添加通配符域名和IP地址
            x509.DNSName("*.local"),
        ]),
        critical=False
    ).sign(private_key, hashes.SHA256(), default_backend())
    
    # 将私钥和证书写入文件
    with open(KEY_FILE, "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))
    
    with open(CERT_FILE, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    
    logging.info(f"证书生成完成: {CERT_FILE}")
    return str(CERT_FILE), str(KEY_FILE)

def get_cert_paths() -> Dict[str, str]:
    """
    获取证书和密钥文件路径
    
    返回:
        Dict[str, str]: 包含证书和密钥文件路径的字典
    """
    cert_path, key_path = generate_self_signed_cert()
    return {
        "certfile": cert_path,
        "keyfile": key_path
    }