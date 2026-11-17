from fastapi import APIRouter, Depends

from payments.api.deps import require_api_key
from payments.api.v1 import payments

api_router = APIRouter(dependencies=[Depends(require_api_key)])
api_router.include_router(payments.router, prefix='/payments', tags=['payments'])
