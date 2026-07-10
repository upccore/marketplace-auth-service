from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.ports.repositories import UserRepository
from src.domain.entities import User
from src.infrastructure.persistence.models import UserModel


class SQLAlchemyUserRepository(UserRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, name: str, email: str, password: str) -> User:
        model = UserModel(name=name, email=email, password=password)
        self._session.add(model)
        await self._session.flush()
        return _to_entity(model)

    async def get_by_id(self, user_id: int) -> User | None:
        model = await self._session.get(UserModel, user_id)
        if model is None:
            return None
        return _to_entity(model)

    async def get_by_email(self, email: str) -> User | None:
        result = await self._session.execute(
            select(UserModel).where(UserModel.email == email)
        )
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return _to_entity(model)

    async def delete(self, user_id: int) -> bool:
        model = await self._session.get(UserModel, user_id)
        if model is None:
            return False
        await self._session.delete(model)
        await self._session.flush()
        return True


def _to_entity(model: UserModel) -> User:
    return User(
        id=model.id,
        name=model.name,
        email=model.email,
        password=model.password,
        created_at=model.created_at,
    )
